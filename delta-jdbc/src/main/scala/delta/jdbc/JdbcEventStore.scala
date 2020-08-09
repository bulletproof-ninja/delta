package delta.jdbc

import java.sql.{ Connection, ResultSet, SQLException }

import scala.concurrent._, duration._
import scala.util.{ Failure, Success, Try }

import delta._

import scuff.StreamConsumer
import scuff.concurrent._
import scuff.jdbc.AsyncConnectionSource

private object JdbcEventStore {
  final class RawEvent[SF](name: String, version: Byte, data: SF) {
    def decode[EVT](channel: delta.Channel, metadata: Map[String, String])(
        implicit
        evtFmt: EventFormat[EVT, SF]): EVT =
      evtFmt.decode(name, version, data, channel, metadata)
  }
}

abstract class JdbcEventStore[ID, EVT, SF](
    evtFmt: EventFormat[EVT, SF],
    dialect: Dialect[ID, EVT, SF], cs: AsyncConnectionSource)(
    initTicker: JdbcEventStore[ID, EVT, SF] => Ticker)
  extends EventStore[ID, EVT] {

  lazy val ticker = initTicker(this)

  @inline implicit private def ef = evtFmt

  def ensureSchema(ensureSchema: Boolean = true): this.type = {
    if (ensureSchema) {
      cs.asyncUpdate { conn =>
        dialect.createSchema(conn)
        dialect.createStreamTable(conn)
        dialect.createChannelIndex(conn)
        dialect.createTransactionTable(conn)
        dialect.createTickIndex(conn)
        dialect.createEventTable(conn)
        dialect.createEventNameIndex(conn)
        dialect.createMetadataTable(conn)
      }
    }.await(33.seconds)
    this
  }

  private def selectRevision(stream: ID, revision: Revision)(
      implicit
      conn: Connection): Option[Transaction] = {
    var dupe: Option[Transaction] = None
    dialect.selectStreamRevision(stream, revision) {
      case (rs, col) =>
        if (rs.next) {
          processTransactions(singleStream = true, tx => dupe = Some(tx))(stream, revision, rs, col)
        }
    }
    assert {
      dupe.forall { tx =>
        tx.stream == stream &&
          tx.revision == revision
      }
    }
    dupe
  }

  def commit(
      channel: Channel, stream: ID, revision: Revision, tick: Tick,
      events: List[EVT], metadata: Map[String, String] = Map.empty): Future[Transaction] = {
    require(revision >= 0, "Must be non-negative revision, was: " + revision)
    require(events.nonEmpty, "Must have at least one event")
    cs.asyncUpdate { implicit conn =>
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
      Transaction(tick, channel, stream, revision, metadata, events)
    }
  }

  def currRevision(stream: ID): Future[Option[Int]] =
    cs.asyncQuery { implicit conn =>
      dialect.selectMaxRevision(stream)
    }

  def maxTick: Future[Option[Long]] =
    cs.asyncQuery(dialect.selectMaxTick(_))

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

  private def complete[U](cb: StreamConsumer[Transaction, U])(thunk: => Future[Unit]): Unit = {
    import cs.queryContext
    thunk.onComplete {
      case Success(_) => cb.onDone()
      case Failure(th) => cb onError th
    }
  }

  @annotation.tailrec
  private def processTransactions(singleStream: Boolean, onNext: Transaction => Unit)(
      stream: ID, revision: Revision, rs: ResultSet, col: dialect.Columns): Unit = {
    import JdbcEventStore.RawEvent
    val channel = rs.getChannel(col.channel)
    val tick = rs.getLong(col.tick)
    var lastEvtIdx: Byte = -1
    var metadata = Map.empty[String, String]
    var rawEvents = List.empty[RawEvent[SF]]
    var continue = false
    var nextTxKey: Option[(ID, Int)] = None
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
            nextTxKey = nextRow
          }
      }
      continue = nextRow.isDefined
    } while (continue && nextTxKey.isEmpty)
    val events = rawEvents.foldLeft(List.empty[EVT]) {
      case (list, rawEvent) => rawEvent.decode(channel, metadata) :: list
    }
    onNext(Transaction(tick, channel, stream, revision, metadata, events))
    nextTxKey match {
      case Some((stream, revision)) =>
        processTransactions(singleStream, onNext)(stream, revision, rs, col)
      case _ => // Done
    }
  }

  def replayStream[R](stream: ID)(callback: StreamConsumer[Transaction, R]): Unit =
    complete(callback) {
      cs.asyncQuery { implicit conn =>
        dialect.selectStreamFull(stream) {
          case (rs, col) =>
            nextRevision(rs, col) foreach { revision =>
              processTransactions(singleStream = true, callback.onNext)(stream, revision, rs, col)
            }
        }
      }
    }
  def replayStreamRange[R](stream: ID, revisionRange: Range)(callback: StreamConsumer[Transaction, R]): Unit =
    complete(callback) {
      cs.asyncQuery { implicit conn =>
        dialect.selectStreamRange(stream, revisionRange) {
          case (rs, col) =>
            nextRevision(rs, col) foreach { revision =>
              processTransactions(singleStream = true, callback.onNext)(stream, revision, rs, col)
            }
        }
      }
    }
  def replayStreamFrom[R](stream: ID, fromRevision: Revision)(callback: StreamConsumer[Transaction, R]): Unit =
    if (fromRevision == 0) replayStream(stream)(callback)
    else complete(callback) {
      cs.asyncQuery { implicit conn =>
        dialect.selectStreamRange(stream, fromRevision to Int.MaxValue) {
          case (rs, col) =>
            nextRevision(rs, col) foreach { revision =>
              processTransactions(singleStream = true, callback.onNext)(stream, revision, rs, col)
            }
        }
      }
    }

  def query[U](selector: Selector)(callback: StreamConsumer[Transaction, U]): Unit =
    complete(callback) {
      cs.asyncQuery { implicit conn =>
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
  def querySince[U](sinceTick: Tick, selector: Selector)(callback: StreamConsumer[Transaction, U]): Unit =
    complete(callback) {
      cs.asyncQuery { implicit conn =>
        val (singleStream, onNext, select) = selector match {
          case Everything =>
            (None, (callback.onNext _), dialect.selectTransactions(sinceTick) _)
          case ChannelSelector(channels) =>
            (None, (callback.onNext _), dialect.selectTransactionsByChannels(channels, sinceTick) _)
          case EventSelector(byChannel) =>
            (None, (callback.onNext _), dialect.selectTransactionsByEvents(byChannel, sinceTick) _)
          case SingleStreamSelector(id, _) =>
            val onNext = (tx: Transaction) => if (tx.tick >= sinceTick) callback.onNext(tx)
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
