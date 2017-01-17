package delta.jdbc

import java.sql.{ Connection, ResultSet, SQLException }

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

import scuff.concurrent.{ StreamCallback, Threads }
import delta.{ EventCodec, EventStore }

private object JdbcEventStore {
  lazy val DefaultThreadPool = {
    val tg = Threads.newThreadGroup("JDBC Executors", false)
    val tf = Threads.factory("jdbc-executor", tg)
    Threads.newCachedThreadPool(tf)
  }
}

abstract class JdbcEventStore[ID, EVT, CH, SF](
  dialect: Dialect[ID, EVT, CH, SF],
  blockingJdbcCtx: ExecutionContext = JdbcEventStore.DefaultThreadPool,
  ensureSchema: Boolean = true)(implicit codec: EventCodec[EVT, SF])
    extends EventStore[ID, EVT, CH] {
  cp: ConnectionProvider =>

  if (ensureSchema) ensureSchema()

  protected def ensureSchema(): Unit = {
    forUpdate { conn =>
      dialect.createSchema(conn)
      dialect.createStreamTable(conn)
      dialect.createChannelIndex(conn)
      dialect.createTransactionTable(conn)
      dialect.createTickIndex(conn)
      dialect.createEventTable(conn)
      dialect.createEventNameIndex(conn)
      dialect.createMetadataTable(conn)
    }
  }

  protected def prepareUpdate(conn: Connection) {
    conn.setAutoCommit(false)
    conn.setReadOnly(false)
  }
  protected def prepareQuery(conn: Connection) {
    conn.setReadOnly(true)
  }

  protected def forUpdate[R](thunk: Connection => R): R = {
    useConnection { conn =>
      prepareUpdate(conn)
      thunk(conn)
    }
  }
  protected def forQuery[R](thunk: Connection => R): R = {
    useConnection { conn =>
      prepareQuery(conn)
      thunk(conn)
    }
  }

  private def selectRevision(stream: ID, revision: Int)(
    implicit conn: Connection): Option[TXN] = {
    var dupe: Option[TXN] = None
    dialect.selectStreamRevision(stream, revision) {
      case (rs, col) =>
        if (rs.next) {
          processTransactions(singleStream = true, txn => dupe = Some(txn))(stream, revision, rs, col)
        }
    }
    assert {
      dupe.forall { txn =>
        txn.stream == stream &&
          txn.revision == revision
      }
    }
    dupe
  }

  def commit(
    channel: CH, stream: ID, revision: Int, tick: Long,
    events: List[EVT], metadata: Map[String, String] = Map.empty): Future[TXN] = {
    require(revision >= 0, "Must be non-negative revision, was: " + revision)
    require(events.nonEmpty, "Must have at least one event")
    Future {
      forUpdate { implicit conn =>
        try {
          if (revision == 0) dialect.insertStream(stream, channel)
          dialect.insertTransaction(stream, revision, tick)
          dialect.insertEvents(stream, revision, events)
          dialect.insertMetadata(stream, revision, metadata)
          conn.commit()
        } catch {
          case sqlEx: SQLException if dialect.isDuplicateKeyViolation(sqlEx) =>
            selectRevision(stream, revision) match {
              case Some(dupe) => throw new DuplicateRevisionException(dupe)
              case None => throw sqlEx
            }
        }
      }
      Transaction(tick, channel, stream, revision, metadata, events)
    }(blockingJdbcCtx)
  }

  def currRevision(stream: ID): Future[Option[Int]] = Future {
    forQuery { implicit conn =>
      dialect.selectMaxRevision(stream)
    }
  }(blockingJdbcCtx)

  def lastTick(): Future[Option[Long]] = Future {
    forQuery(dialect.selectMaxTick(_))
  }(blockingJdbcCtx)

  private def nextTransactionKey(rs: ResultSet, col: dialect.Columns): Option[(ID, Int)] = {
    nextRevision(rs, col).map { rev =>
      dialect.idType.readFrom(rs, col.stream_id) -> rev
    }
  }
  private def nextRevision(rs: ResultSet, col: dialect.Columns): Option[Int] = {
    if (rs.next()) Some {
      rs.getInt(col.revision)
    }
    else None
  }

  private def future(cb: StreamCallback[TXN])(thunk: => Unit): Unit =
    Try(thunk) match {
      case Success(_) => cb.onCompleted()
      case Failure(th) => cb onError th
    }

  @annotation.tailrec
  private def processTransactions(singleStream: Boolean, onNext: TXN => Unit)(
    stream: ID, revision: Int, rs: ResultSet, col: dialect.Columns): Unit = {
    val channel = dialect.chType.readFrom(rs, col.channel)
    val tick = rs.getLong(col.tick)
    var lastEvtIdx: Byte = -1
    var metadata = Map.empty[String, String]
    var events = List.empty[EVT]
    var continue = false
    var nextTxnKey: Option[(ID, Int)] = None
    do {
      val evtIdx = rs.getByte(col.event_idx)
      if (evtIdx == 0) { // Metadata repeats per event, so only read on first
        val mdKey = rs.getString(col.metadata_key)
        if (mdKey != null) {
          val mdVal = rs.getString(col.metadata_val)
          metadata = metadata.updated(mdKey, mdVal)
        }
      }
      if (evtIdx != lastEvtIdx) { // New event
        val name = rs.getString(col.event_name)
        val version = rs.getByte(col.event_version)
        val data = dialect.sfType.readFrom(rs, col.event_data)
        events = codec.decode(name, version, data) :: events
      }
      lastEvtIdx = evtIdx
      val nextRow =
        if (singleStream) nextRevision(rs, col).map(stream -> _)
        else nextTransactionKey(rs, col)
      nextRow.foreach {
        case (nextStream, nextRev) if revision != nextRev || stream != nextStream =>
          nextTxnKey = nextRow
      }
      continue = nextRow.isDefined
    } while (continue && nextTxnKey.isEmpty)
    onNext(Transaction(tick, channel, stream, revision, metadata, events.reverse))
    nextTxnKey match {
      case Some((stream, revision)) =>
        processTransactions(singleStream, onNext)(stream, revision, rs, col)
      case _ => // Done
    }
  }

  def replayStream(stream: ID)(callback: StreamCallback[TXN]): Unit =
    future(callback) {
      forQuery { implicit conn =>
        dialect.selectStreamFull(stream) {
          case (rs, col) =>
            nextRevision(rs, col) foreach { revision =>
              processTransactions(singleStream = true, callback.onNext)(stream, revision, rs, col)
            }
        }
      }
    }
  def replayStreamRange(stream: ID, revisionRange: Range)(callback: StreamCallback[TXN]): Unit =
    future(callback) {
      forQuery { implicit conn =>
        dialect.selectStreamRange(stream, revisionRange) {
          case (rs, col) =>
            nextRevision(rs, col) foreach { revision =>
              processTransactions(singleStream = true, callback.onNext)(stream, revision, rs, col)
            }
        }
      }
    }
  def replayStreamFrom(stream: ID, fromRevision: Int)(callback: StreamCallback[TXN]): Unit =
    if (fromRevision == 0) replayStream(stream)(callback)
    else future(callback) {
      forQuery { implicit conn =>
        dialect.selectStreamRange(stream, fromRevision to Int.MaxValue) {
          case (rs, col) =>
            nextRevision(rs, col) foreach { revision =>
              processTransactions(singleStream = true, callback.onNext)(stream, revision, rs, col)
            }
        }
      }
    }

  def query(selector: Selector)(callback: StreamCallback[TXN]): Unit =
    future(callback) {
      forQuery { implicit conn =>
        val select = selector match {
          case Everything => dialect.selectTransactions() _
          case ChannelSelector(channels) => dialect.selectTransactionsByChannels(channels) _
          case EventSelector(byChannel) => dialect.selectTransactionsByEvents(byChannel) _
          case StreamSelector(id, _) => dialect.selectStreamFull(id) _
        }
        select {
          case (rs, col) =>
            nextTransactionKey(rs, col) foreach {
              case (stream, revision) =>
                processTransactions(singleStream = false, callback.onNext)(stream, revision, rs, col)
            }
        }
      }
    }
  def querySince(sinceTick: Long, selector: Selector)(callback: StreamCallback[TXN]): Unit =
    future(callback) {
      forQuery { implicit conn =>
        import Selector._
        val (singleStream, onNext, select) = selector match {
          case Everything =>
            (None, (callback.onNext _), dialect.selectTransactions(sinceTick) _)
          case ChannelSelector(channels) =>
            (None, (callback.onNext _), dialect.selectTransactionsByChannels(channels, sinceTick) _)
          case EventSelector(byChannel) =>
            (None, (callback.onNext _), dialect.selectTransactionsByEvents(byChannel, sinceTick) _)
          case StreamSelector(id, _) =>
            val onNext = (txn: TXN) => if (txn.tick >= sinceTick) callback.onNext(txn)
            (Some(id), onNext, dialect.selectStreamFull(id) _)
        }
        select {
          case (rs, col) =>
            val next = singleStream flatMap { id =>
              nextRevision(rs, col).map(id -> _)
            } orElse nextTransactionKey(rs, col)
            next foreach {
              case (stream, revision) =>
                processTransactions(singleStream.isDefined, onNext)(stream, revision, rs, col)
            }
        }
      }
    }

}
