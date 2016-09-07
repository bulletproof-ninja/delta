package ulysses.jdbc

import ulysses._
import collection.{ Seq => aSeq, Map => aMap }
import scala.reflect.{ ClassTag, classTag }
import java.util.{ UUID, List => JList, ArrayList }
import scuff.concurrent._
import scala.concurrent._
import scala.util.{ Try, Success, Failure }
import collection.JavaConverters._
import java.sql.Connection
import javax.sql.DataSource
import java.sql.PreparedStatement
import scala.util.control.NonFatal
import java.sql.SQLException
import java.sql.ResultSet

class JdbcEventStore[ID: ColumnType, EVT, CH: ColumnType, SF: ColumnType](
  dataSource: DataSource,
  dialect: Dialect[ID, EVT, CH, SF])(
    implicit blockingJdbcCtx: ExecutionContext)
    extends EventStore[ID, EVT, CH] {

  protected final def getChannel(cls: Class[_ <: EVT]) = dialect.evtCtx.channel(cls)

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
  protected def useConnection[R](thunk: Connection => R): R = {
    val conn = dataSource.getConnection
    try {
      thunk(conn)
    } finally {
      conn.close()
    }
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
        processTransactions(singleStream = true, txn => dupe = Some(txn))(stream, revision, rs, col)
    }
    assert {
      dupe.forall { txn =>
        txn.stream == stream &&
          txn.revision == revision
      }
    }
    dupe
  }

  def record(
    channel: CH, stream: ID, revision: Int, tick: Long,
    events: aSeq[EVT], metadata: aMap[String, String] = Map.empty): Future[TXN] = {
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
    }
  }

  def currRevision(stream: ID): Future[Option[Int]] = Future {
    forQuery { implicit conn =>
      dialect.selectMaxRevision(stream)
    }
  }
  def lastTick(): Future[Option[Long]] = Future {
    forQuery(dialect.selectMaxTick(_))
  }

  private def nextTransactionKey(rs: ResultSet, col: dialect.Columns): Option[(ID, Int)] = {
    nextRevision(rs, col).map { rev =>
      dialect.ct[ID].readFrom(rs, col.stream_id) -> rev
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
    val channel = dialect.ct[CH].readFrom(rs, col.channel)
    val tick = rs.getLong(col.tick)
    var lastEvtIdx: Byte = -1
    var metadata = Map.empty[String, String]
    var events = Vector.empty[EVT]
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
        val version = rs.getShort(col.event_version)
        val data = dialect.ct[SF].readFrom(rs, col.event_data)
        events :+= dialect.evtCtx.decode(name, version, data)
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
    onNext(Transaction(tick, channel, stream, revision, metadata, events))
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

  def replay(filter: StreamFilter[ID, EVT, CH])(callback: StreamCallback[TXN]): Unit =
    future(callback) {
      import StreamFilter._
      forQuery { implicit conn =>
        val select = filter match {
          case Everything() => dialect.selectTransactions() _
          case ByChannel(channels) => dialect.selectTransactionsByChannels(channels) _
          case ByEvent(events) => dialect.selectTransactionsByEvents(events) _
          case ByStream(id, _) => dialect.selectStreamFull(id) _
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
  def replaySince(sinceTick: Long, filter: StreamFilter[ID, EVT, CH])(callback: StreamCallback[TXN]): Unit =
    future(callback) {
      forQuery { implicit conn =>
        import StreamFilter._
        val (singleStream, onNext, select) = filter match {
          case Everything() =>
            (None, (callback.onNext _), dialect.selectTransactions(sinceTick) _)
          case ByChannel(channels) =>
            (None, (callback.onNext _), dialect.selectTransactionsByChannels(channels, sinceTick) _)
          case ByEvent(events) =>
            (None, (callback.onNext _), dialect.selectTransactionsByEvents(events, sinceTick) _)
          case ByStream(id, _) =>
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
