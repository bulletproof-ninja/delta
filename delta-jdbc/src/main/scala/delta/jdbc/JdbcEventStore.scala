package delta.jdbc

import java.sql.{ Connection, ResultSet, SQLException }

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

import delta.{ EventFormat, EventStore }
import scuff.StreamConsumer
import scuff.jdbc.ConnectionSource
import delta.Ticker

private object JdbcEventStore {
  final class RawEvent[SF](name: String, version: Byte, data: SF) {
    def decode[EVT](channel: delta.Transaction.Channel, metadata: Map[String, String])(
        implicit
        evtFmt: EventFormat[EVT, SF]): EVT =
      evtFmt.decode(name, version, data, channel, metadata)
  }
}

class JdbcEventStore[ID, EVT, SF](
    evtFmt: EventFormat[EVT, SF],
    dialect: Dialect[ID, EVT, SF], cs: ConnectionSource,
    blockingJdbcCtx: ExecutionContext)(
    initTicker: JdbcEventStore[ID, EVT, SF] => Ticker)
  extends EventStore[ID, EVT] {

  lazy val ticker = initTicker(this)
  
  @inline implicit private def ef = evtFmt

  def ensureSchema(): this.type = {
    cs.forUpdate { conn =>
      dialect.createSchema(conn)
      dialect.createStreamTable(conn)
      dialect.createChannelIndex(conn)
      dialect.createTransactionTable(conn)
      dialect.createTickIndex(conn)
      dialect.createEventTable(conn)
      dialect.createEventNameIndex(conn)
      dialect.createMetadataTable(conn)
    }
    this
  }

  private def selectRevision(stream: ID, revision: Int)(
      implicit
      conn: Connection): Option[TXN] = {
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
      channel: Channel, stream: ID, revision: Int, tick: Long,
      events: List[EVT], metadata: Map[String, String] = Map.empty): Future[TXN] = {
    require(revision >= 0, "Must be non-negative revision, was: " + revision)
    require(events.nonEmpty, "Must have at least one event")
    Future {
      cs.forUpdate { implicit conn =>
        try {
          if (revision == 0) dialect.insertStream(stream, channel)
          dialect.insertTransaction(stream, revision, tick)
          dialect.insertEvents(stream, revision, events)
          dialect.insertMetadata(stream, revision, metadata)
        } catch {
          case sqlEx: SQLException if dialect.isDuplicateKeyViolation(sqlEx) =>
            Try(conn.rollback)
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
    cs.forQuery { implicit conn =>
      dialect.selectMaxRevision(stream)
    }
  }(blockingJdbcCtx)

  def maxTick(): Future[Option[Long]] = Future {
    cs.forQuery(dialect.selectMaxTick(_))
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

  private def FutureWith[U](cb: StreamConsumer[TXN, U])(thunk: => Unit): Unit = Future {
    Try(thunk) match {
      case Success(_) => cb.onDone()
      case Failure(th) => cb onError th
    }
  }(blockingJdbcCtx)

  @annotation.tailrec
  private def processTransactions(singleStream: Boolean, onNext: TXN => Unit)(
      stream: ID, revision: Int, rs: ResultSet, col: dialect.Columns): Unit = {
    import JdbcEventStore.RawEvent
    val channel = rs.getChannel(col.channel)
    val tick = rs.getLong(col.tick)
    var lastEvtIdx: Byte = -1
    var metadata = Map.empty[String, String]
    var rawEvents = List.empty[RawEvent[SF]]
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
        rawEvents = new RawEvent(name, version, data) :: rawEvents
      }
      lastEvtIdx = evtIdx
      val nextRow =
        if (singleStream) nextRevision(rs, col).map(stream -> _)
        else nextTransactionKey(rs, col)
      nextRow foreach {
        case (nextStream, nextRev) =>
          if (revision != nextRev || stream != nextStream) {
            nextTxnKey = nextRow
          }
      }
      continue = nextRow.isDefined
    } while (continue && nextTxnKey.isEmpty)
    val events = rawEvents.foldLeft(List.empty[EVT]) {
      case (list, rawEvent) => rawEvent.decode(channel, metadata) :: list
    }
    onNext(Transaction(tick, channel, stream, revision, metadata, events))
    nextTxnKey match {
      case Some((stream, revision)) =>
        processTransactions(singleStream, onNext)(stream, revision, rs, col)
      case _ => // Done
    }
  }

  def replayStream[R](stream: ID)(callback: StreamConsumer[TXN, R]): Unit =
    FutureWith(callback) {
      cs.forQuery { implicit conn =>
        dialect.selectStreamFull(stream) {
          case (rs, col) =>
            nextRevision(rs, col) foreach { revision =>
              processTransactions(singleStream = true, callback.onNext)(stream, revision, rs, col)
            }
        }
      }
    }
  def replayStreamRange[R](stream: ID, revisionRange: Range)(callback: StreamConsumer[TXN, R]): Unit =
    FutureWith(callback) {
      cs.forQuery { implicit conn =>
        dialect.selectStreamRange(stream, revisionRange) {
          case (rs, col) =>
            nextRevision(rs, col) foreach { revision =>
              processTransactions(singleStream = true, callback.onNext)(stream, revision, rs, col)
            }
        }
      }
    }
  def replayStreamFrom[R](stream: ID, fromRevision: Int)(callback: StreamConsumer[TXN, R]): Unit =
    if (fromRevision == 0) replayStream(stream)(callback)
    else FutureWith(callback) {
      cs.forQuery { implicit conn =>
        dialect.selectStreamRange(stream, fromRevision to Int.MaxValue) {
          case (rs, col) =>
            nextRevision(rs, col) foreach { revision =>
              processTransactions(singleStream = true, callback.onNext)(stream, revision, rs, col)
            }
        }
      }
    }

  def query[U](selector: Selector)(callback: StreamConsumer[TXN, U]): Unit =
    FutureWith(callback) {
      cs.forQuery { implicit conn =>
        val select = selector match {
          case Everything => dialect.selectTransactions() _
          case ChannelSelector(channels) => dialect.selectTransactionsByChannels(channels) _
          case EventSelector(byChannel) => dialect.selectTransactionsByEvents(byChannel) _
          case SingleStreamSelector(id, _) => dialect.selectStreamFull(id) _
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
  def querySince[U](sinceTick: Long, selector: Selector)(callback: StreamConsumer[TXN, U]): Unit =
    FutureWith(callback) {
      cs.forQuery { implicit conn =>
        val (singleStream, onNext, select) = selector match {
          case Everything =>
            (None, (callback.onNext _), dialect.selectTransactions(sinceTick) _)
          case ChannelSelector(channels) =>
            (None, (callback.onNext _), dialect.selectTransactionsByChannels(channels, sinceTick) _)
          case EventSelector(byChannel) =>
            (None, (callback.onNext _), dialect.selectTransactionsByEvents(byChannel, sinceTick) _)
          case SingleStreamSelector(id, _) =>
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
