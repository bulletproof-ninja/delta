package delta.jdbc

import java.sql.{ Connection, ResultSet, SQLException }

import scala.concurrent._, duration._
import scala.util.Try

import delta._

import scuff.Reduction
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
  dialect: Dialect[ID, EVT, SF], cs: AsyncConnectionSource)
extends EventStore[ID, EVT] {

  @inline implicit private def ef = evtFmt

  protected def asyncEnsureSchema(): Future[Unit] =
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

  /** Ensure schema tables exists. */
  def ensureSchema(ensureSchema: Boolean = true): this.type = {
    if (ensureSchema) {
      asyncEnsureSchema().await(60.seconds)
    }
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

  protected def commit(
      tick: Tick,
      channel: Channel,
      stream: ID,
      revision: Revision,
      metadata: Map[String, String],
      events: List[EVT])
      : Future[Transaction] = {
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

  private def complete[U](reduction: Reduction[Transaction, U])(thunk: => Future[Unit]): Future[U] = {
    import cs.queryContext
    thunk.map { _ =>
      reduction.result()
    }
  }

  @annotation.tailrec
  private def processTransactions(singleStream: Boolean, next: Transaction => Unit)(
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
    next(Transaction(tick, channel, stream, revision, metadata, events))
    nextTxKey match {
      case Some((stream, revision)) =>
        processTransactions(singleStream, next)(stream, revision, rs, col)
      case _ => // Done
    }
  }

  def replayStream[R](stream: ID)(reduction: Reduction[Transaction, R]): Future[R] =
    complete(reduction) {
      cs.asyncQuery { implicit conn =>
        dialect.selectStreamFull(stream) {
          case (rs, col) =>
            nextRevision(rs, col) foreach { revision =>
              processTransactions(singleStream = true, reduction.next)(stream, revision, rs, col)
            }
        }
      }
    }
  def replayStreamRange[R](stream: ID, revisionRange: Range)(reduction: Reduction[Transaction, R]): Future[R] =
    complete(reduction) {
      cs.asyncQuery { implicit conn =>
        dialect.selectStreamRange(stream, revisionRange) {
          case (rs, col) =>
            nextRevision(rs, col) foreach { revision =>
              processTransactions(singleStream = true, reduction.next)(stream, revision, rs, col)
            }
        }
      }
    }
  def replayStreamFrom[R](stream: ID, fromRevision: Revision)(reduction: Reduction[Transaction, R]): Future[R] =
    if (fromRevision == 0) replayStream(stream)(reduction)
    else complete(reduction) {
      cs.asyncQuery { implicit conn =>
        dialect.selectStreamRange(stream, fromRevision to Int.MaxValue) {
          case (rs, col) =>
            nextRevision(rs, col) foreach { revision =>
              processTransactions(singleStream = true, reduction.next)(stream, revision, rs, col)
            }
        }
      }
    }

  def query[U](selector: Selector)(reduction: Reduction[Transaction, U]): Future[U] =
    complete(reduction) {
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
                processTransactions(singleStream = false, reduction.next)(stream, revision, rs, col)
            }
        }
      }
    }
  def querySince[U](sinceTick: Tick, selector: Selector)(reduction: Reduction[Transaction, U]): Future[U] =
    complete(reduction) {
      cs.asyncQuery { implicit conn =>
        val (singleStream, reduce, select) = selector match {
          case Everything =>
            (None, (reduction.next _), dialect.selectTransactions(sinceTick) _)
          case ChannelSelector(channels) =>
            (None, (reduction.next _), dialect.selectTransactionsByChannels(channels, sinceTick) _)
          case EventSelector(byChannel) =>
            (None, (reduction.next _), dialect.selectTransactionsByEvents(byChannel, sinceTick) _)
          case SingleStreamSelector(id, _) =>
            val reduce = (tx: Transaction) => if (tx.tick >= sinceTick) reduction.next(tx)
            (Some(id), reduce, dialect.selectStreamFull(id) _)
        }
        select {
          case (rs, col) =>
            val next = singleStream flatMap { id =>
              nextRevision(rs, col).map(id -> _)
            } orElse nextTransactionKey(rs, col)
            next foreach {
              case (stream, revision) =>
                processTransactions(singleStream.isDefined, reduce)(stream, revision, rs, col)
            }
        }
      }
    }

}
