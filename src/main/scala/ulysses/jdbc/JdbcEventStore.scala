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

class JdbcEventStore[ID: TypeConverter, EVT, CH: TypeConverter, SF: TypeConverter](
  dataSource: DataSource,
  dialect: Dialect[ID, EVT, CH, SF])(
    implicit blockingJdbcCtx: ExecutionContext)
    extends EventStore[ID, EVT, CH] {

  protected final def evtCtx = dialect.evtCtx

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

  def record(
    channel: CH, stream: ID, revision: Int, tick: Long,
    events: aSeq[EVT], metadata: aMap[String, String] = Map.empty): Future[TXN] = {
    require(revision >= 0, "Must be non-negative revision, was: " + revision)
    require(events.nonEmpty, "Must have at least one event")
    Future {
      forUpdate { implicit conn =>
        if (revision == 0) {
          try dialect.insertStream(stream, channel) catch {
            case sqlEx: SQLException if dialect.isDuplicateKeyViolation(sqlEx) =>
              throw new DuplicateRevisionException(null) // <- FIXME!!!!
          }
        }
        dialect.insertTransaction(stream, revision, tick)
        dialect.insertEvents(stream, revision, events)
        dialect.insertMetadata(stream, revision, metadata)
        conn.commit()
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

  def replayStream(stream: ID)(callback: StreamCallback[TXN]): Unit = Future {
    forQuery { implicit conn =>
      dialect.selectStreamFull(stream) {
        case (rs, col) => try {
          if (rs.next) {
            val channel = dialect.tc[CH].readFrom(rs, col.channel)
            var lastRev = -1
            var lastEvtIdx: Byte = -1
            var metadata = Map.empty[String, String]
            var events = Vector.empty[EVT]
            var tick: Long = 0
            do {
              val revision = rs.getInt(col.revision)
              val evtIdx = rs.getByte(col.event_idx)
              if (revision != lastRev) { // New transaction, reset vars
                if (events.nonEmpty) { // Emit any previous transaction
                  callback onNext Transaction(tick, channel, stream, revision, metadata, events)
                }
                lastEvtIdx = -1
                tick = rs.getLong(col.tick)
                events = Vector.empty
                metadata = Map.empty
              }
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
                val data = dialect.tc[SF].readFrom(rs, col.event_data)
                events :+= evtCtx.decode(name, version, data)
              }
              callback onNext Transaction(tick, channel, stream, revision, metadata, events)
              lastRev = revision
              lastEvtIdx = evtIdx
            } while (rs.next)
            // Emit last transaction
            callback onNext Transaction(tick, channel, stream, lastRev, metadata, events)
          }
          callback.onCompleted()
        } catch {
          case NonFatal(th) => callback onError th
        }
      }
    }
  }
  def replayStreamRange(stream: ID, revisionRange: Range)(callback: StreamCallback[TXN]): Unit
  def replayStreamFrom(stream: ID, fromRevision: Int)(callback: StreamCallback[TXN]): Unit

  def replay(filter: StreamFilter[ID, EVT, CH])(callback: StreamCallback[TXN]): Unit
  def replaySince(sinceTick: Long, filter: StreamFilter[ID, EVT, CH])(callback: StreamCallback[TXN]): Unit

}
