package delta.jdbc

import java.sql._

import scuff._
import delta._
import scala.util.Try

class DefaultDialect[ID: ColumnType, EVT, SF: ColumnType](schema: String = null)
  extends Dialect[ID, EVT, SF](schema.optional)

private[jdbc] object Dialect {
  def isDuplicateKeyViolation(sqlEx: SQLException): Boolean = {
    sqlEx.isInstanceOf[SQLIntegrityConstraintViolationException] ||
      Option(sqlEx.getSQLState).exists(_ startsWith "23")
  }

  def executeDDL(conn: Connection, ddl: String): Unit = {
    val stm = conn.createStatement()
    try stm.execute(ddl) catch {
      case cause: SQLException =>
        throw new SQLException(s"Failed to execute DDL: $ddl", cause.getSQLState, cause.getErrorCode, cause)
    } finally stm.close()
  }

  def schemaDDL(name: String): String = s"CREATE SCHEMA IF NOT EXISTS $name"

}

protected class Dialect[ID: ColumnType, EVT, SF: ColumnType] protected[jdbc] (
    final val schema: Option[String]) {

  import Dialect.executeDDL

  protected type Channel = delta.Channel

  private[jdbc] def idType = implicitly[ColumnType[ID]]
  private[jdbc] def sfType = implicitly[ColumnType[SF]]

  def isDuplicateKeyViolation(sqlEx: SQLException): Boolean = Dialect.isDuplicateKeyViolation(sqlEx)

  private[this] val _schemaPrefix = schema.map(_ + ".") getOrElse ""
  protected def schemaPrefix = _schemaPrefix
  private[this] val _streamTable = s"${schemaPrefix}delta_stream"
  protected def streamTable = _streamTable
  private[this] val _transactionTable = s"${schemaPrefix}delta_transaction"
  protected def transactionTable = _transactionTable
  private[this] val _eventTable = s"${schemaPrefix}delta_event"
  protected def eventTable = _eventTable
  private[this] val _metadataTable = s"${schemaPrefix}delta_metadata"
  protected def metadataTable = _metadataTable
  private[this] val _channelIndex = s"${streamTable.replace(".", "_")}_channel_idx"
  protected def channelIndex = _channelIndex
  private[this] val _eventNameIndex = s"${eventTable.replace(".", "_")}_event_idx"
  protected def eventNameIndex = _eventNameIndex
  private[this] val _tickIndex = s"${transactionTable.replace(".", "_")}_tick_idx"
  protected def tickIndex = _tickIndex

  protected def byteDataType = "TINYINT"

  protected def schemaDDL(name: String): String = Dialect.schemaDDL(name)
  def createSchema(conn: Connection): Unit = schema.foreach(schema => executeDDL(conn, schemaDDL(schema)))

  protected def streamTableDDL: String = s"""
CREATE TABLE IF NOT EXISTS $streamTable (
  stream_id ${idType.typeName} NOT NULL,
  channel $channelType NOT NULL,

  PRIMARY KEY (stream_id)
)
  """
  def createStreamTable(conn: Connection): Unit = executeDDL(conn, streamTableDDL)

  protected def channelIndexDDL: String = s"""
CREATE INDEX IF NOT EXISTS $channelIndex
  ON $streamTable (channel)
"""
  def createChannelIndex(conn: Connection): Unit = executeDDL(conn, channelIndexDDL)

  protected def transactionTableDDL: String = s"""
CREATE TABLE IF NOT EXISTS $transactionTable (
  stream_id ${idType.typeName} NOT NULL,
  revision INT NOT NULL,
  tick BIGINT NOT NULL,

  PRIMARY KEY (stream_id, revision),
  FOREIGN KEY (stream_id)
    REFERENCES $streamTable (stream_id)
)
"""
  def createTransactionTable(conn: Connection): Unit = executeDDL(conn, transactionTableDDL)

  protected def tickIndexDDL: String = s"""
CREATE INDEX IF NOT EXISTS $tickIndex
  ON $transactionTable (tick)
"""
  def createTickIndex(conn: Connection): Unit = executeDDL(conn, tickIndexDDL)

  protected def channelType = "VARCHAR(255)"
  protected def eventNameType = "VARCHAR(255)"
  protected def eventTableDDL: String = s"""
CREATE TABLE IF NOT EXISTS $eventTable (
  stream_id ${idType.typeName} NOT NULL,
  revision INT NOT NULL,
  event_idx $byteDataType NOT NULL,
  event_name $eventNameType NOT NULL,
  event_version $byteDataType NOT NULL,
  event_data ${sfType.typeName} NOT NULL,

  PRIMARY KEY (stream_id, revision, event_idx),
  FOREIGN KEY (stream_id, revision)
    REFERENCES $transactionTable (stream_id, revision)
)
"""
  def createEventTable(conn: Connection): Unit = executeDDL(conn, eventTableDDL)

  protected def eventNameIndexDDL: String = s"""
CREATE INDEX IF NOT EXISTS $eventNameIndex
  ON $eventTable (event_name)
"""
  def createEventNameIndex(conn: Connection): Unit = executeDDL(conn, eventNameIndexDDL)

  protected def metadataKeyType = "VARCHAR(255)"
  protected def metadataValType = "VARCHAR(32767)"

  protected def metadataTableDDL: String = s"""
CREATE TABLE IF NOT EXISTS $metadataTable (
  stream_id ${idType.typeName} NOT NULL,
  revision INT NOT NULL,
  metadata_key $metadataKeyType NOT NULL,
  metadata_val $metadataValType NOT NULL,

  PRIMARY KEY (stream_id, revision, metadata_key),
  FOREIGN KEY (stream_id, revision)
    REFERENCES $transactionTable (stream_id, revision)
)
"""
  def createMetadataTable(conn: Connection): Unit = executeDDL(conn, metadataTableDDL)

  private def executeQuery[R](ps: PreparedStatement)(thunk: ResultSet => R): R = {
    val rs = ps.executeQuery()
    try thunk(rs) finally Try(rs.close)
  }
  protected val streamInsert: String = s"""
INSERT INTO $streamTable
  (stream_id, channel)
  VALUES (?, ?)
"""
  def insertStream(stream: ID, channel: Channel)(
      implicit conn: Connection): Unit = {
    conn.prepare(streamInsert) { ps =>
      ps.setValue(1, stream)
      ps.setString(2, channel.toString)
      ps.executeUpdate()
    }
  }
  protected val transactionInsert: String = s"""
INSERT INTO $transactionTable
  (stream_id, revision, tick)
  VALUES (?, ?, ?)
"""
  def insertTransaction(stream: ID, rev: Revision, tick: Tick)(
      implicit conn: Connection): Unit = {
    conn.prepare(transactionInsert) { ps =>
      ps.setValue(1, stream)
      ps.setInt(2, rev)
      ps.setLong(3, tick)
      ps.executeUpdate()
    }
  }
  protected val eventInsert: String = s"""
INSERT INTO $eventTable
  (stream_id, revision, event_idx, event_name, event_version, event_data)
  VALUES (?, ?, ?, ?, ?, ?)
"""
  def insertEvents(stream: ID, rev: Revision, events: List[EVT])(
      implicit conn: Connection, evtFmt: EventFormat[EVT, SF]): Unit = {
    conn.prepare(eventInsert) { ps =>
      val isBatch = events.tail.nonEmpty
      ps.setValue(1, stream)
      ps.setInt(2, rev)
      events.iterator.zipWithIndex.foreach {
        case (evt, idx) =>
          val EventFormat.EventSig(name, version) = evtFmt signature evt
          ps.setByte(3, idx.toByte)
          ps.setString(4, name)
          ps.setByte(5, version)
          ps.setValue(6, evtFmt encode evt)
          if (isBatch) ps.addBatch()
      }
      if (isBatch) ps.executeBatch()
      else ps.executeUpdate()
    }
  }
  protected val metadataInsert: String = s"""
INSERT INTO $metadataTable
  (stream_id, revision, metadata_key, metadata_val)
  VALUES (?, ?, ?, ?)
"""
  def insertMetadata(stream: ID, rev: Revision, metadata: Map[String, String])(
      implicit conn: Connection) = if (metadata.nonEmpty) {
    conn.prepare(metadataInsert) { ps =>
      val isBatch = metadata.size > 1
      ps.setValue(1, stream)
      ps.setInt(2, rev)
      metadata.foreach {
        case (key, value) =>
          ps.setString(3, key)
          ps.setString(4, value)
          if (isBatch) ps.addBatch()
      }
      if (isBatch) ps.executeBatch()
      else ps.executeUpdate()
    }
  }

  private val StreamColumnsPrefixed = Seq(
    "s.channel", "t.tick", "t.revision",
    "e.event_idx", "e.event_name", "e.event_version", "e.event_data",
    "m.metadata_key", "m.metadata_val")
  private val TxColumnsPrefixed = "s.stream_id" +: StreamColumnsPrefixed
  private val StreamColumnsSelect: String = StreamColumnsPrefixed.map { colName =>
    s"$colName AS ${colName.substring(2)}"
  }.mkString(",")
  private val TxColumnsSelect: String = TxColumnsPrefixed.map { colName =>
    s"$colName AS ${colName.substring(2)}"
  }.mkString(",")
  private val StreamColumnsIdx = Columns(StreamColumnsPrefixed.map(_.substring(2)).indexOf(_) + 1)
  private val TxColumnsIdx = Columns(TxColumnsPrefixed.map(_.substring(2)).indexOf(_) + 1)

  private[jdbc] final case class Columns(
      stream_id: Int, revision: Revision,
      tick: Int, channel: Int,
      event_idx: Int, event_name: Int, event_version: Int, event_data: Int,
      metadata_key: Int, metadata_val: Int)
  private object Columns {
    def apply(colIdx: String => Int): Columns = {
      new Columns(
        stream_id = colIdx("stream_id"),
        revision = colIdx("revision"),
        tick = colIdx("tick"),
        channel = colIdx("channel"),
        event_idx = colIdx("event_idx"),
        event_name = colIdx("event_name"),
        event_version = colIdx("event_version"),
        event_data = colIdx("event_data"),
        metadata_key = colIdx("metadata_key"),
        metadata_val = colIdx("metadata_val"))
    }
  }

  protected val maxRevisionQuery = s"""
SELECT MAX(revision)
FROM $transactionTable
WHERE stream_id = ?
GROUP BY stream_id
"""
  def selectMaxRevision(stream: ID)(implicit conn: Connection): Option[Int] = {
    conn.prepare(maxRevisionQuery) { ps =>
      ps.setFetchSize(1)
      ps.setValue(1, stream)
      executeQuery(ps) { rs =>
        if (rs.next) Some(rs.getInt(1))
        else None
      }
    }
  }

  protected def maxTickQuery = s"""
SELECT MAX(tick)
FROM $transactionTable
"""
  def selectMaxTick(implicit conn: Connection): Option[Long] = {
    val stm = conn.createStatement()
    try {
      val rs = stm executeQuery maxTickQuery
      try {
        if (rs.next) Some(rs.getLong(1))
        else None
      } finally {
        Try(rs.close)
      }
    } finally {
      Try(stm.close)
    }
  }

  protected val FromJoin =
s"""FROM $streamTable s
JOIN $transactionTable t
  ON t.stream_id = s.stream_id
JOIN $eventTable e
  ON e.stream_id = t.stream_id
  AND e.revision = t.revision
LEFT OUTER JOIN $metadataTable m
  ON m.stream_id = t.stream_id
  AND m.revision = t.revision"""

  protected def makeTxQuery(WHERE: String = ""): String = s"""
SELECT $TxColumnsSelect
$FromJoin
$WHERE
ORDER BY e.stream_id, e.revision, e.event_idx
"""

  def selectTransactionsByChannels(channels: Set[Channel], sinceTick: Tick = Long.MinValue)(
      thunk: (ResultSet, Columns) => Unit)(
      implicit conn: Connection): Unit = {
    val tickBound = sinceTick != Long.MinValue
    val prefix = if (tickBound) {
      s"WHERE t.tick >= ? AND"
    } else "WHERE"
    val WHERE = s"$prefix ${makeWHEREByChannelsOrEvents(channels.size)}"
    val query = makeTxQuery(WHERE)
    conn.prepare(query) { ps =>
      val colIdx = Iterator.from(1)
      if (tickBound) ps.setLong(colIdx.next(), sinceTick)
      channels foreach { channel =>
        ps.setChannel(colIdx.next(), channel)
      }
      executeQuery(ps) { rs =>
        thunk(rs, TxColumnsIdx)
      }
    }
  }
  def selectTransactionsByEvents(eventsByChannel: Map[Channel, Set[Class[_ <: EVT]]], sinceTick: Tick = Long.MinValue)(
      thunk: (ResultSet, Columns) => Unit)(
      implicit conn: Connection, evtFmt: EventFormat[EVT, SF]): Unit = {
    val tickBound = sinceTick != Long.MinValue
    val prefix = if (tickBound) {
      s"WHERE t.tick >= ? AND"
    } else "WHERE"
    val channels = eventsByChannel.keys
    val events = eventsByChannel.values.flatten
    val WHERE = s"$prefix ${makeWHEREByChannelsOrEvents(channels.size, events.size)}"
    val query = makeTxQuery(WHERE)
    conn.prepare(query) { ps =>
      val colIdx = Iterator.from(1)
      if (tickBound) ps.setLong(colIdx.next(), sinceTick)
      channels foreach { channel =>
        ps.setChannel(colIdx.next(), channel)
      }
      events foreach { evt =>
        ps.setString(colIdx.next(), evtFmt.signature(evt).name)
      }
      executeQuery(ps) { rs =>
        thunk(rs, TxColumnsIdx)
      }
    }
  }
  def selectTransactions(sinceTick: Tick = Long.MinValue)(
      thunk: (ResultSet, Columns) => Unit)(
      implicit conn: Connection): Unit = {
    val tickBound = sinceTick != Long.MinValue
    val query = if (tickBound) {
      makeTxQuery(s"WHERE t.tick >= ?")
    } else {
      makeTxQuery()
    }
    conn.prepare(query) { ps =>
      if (tickBound) ps.setLong(1, sinceTick)
      executeQuery(ps) { rs =>
        thunk(rs, TxColumnsIdx)
      }
    }
  }

  protected def makeWHEREByChannelsOrEvents(chCount: Int, evtCount: Int = 0): String = {
    val whereEventName =
      if (evtCount == 1) "WHERE e2.event_name = ?"
      else {
        val parms = Iterator.fill(evtCount)("?")
        parms.mkString("WHERE e2.event_name IN (", ",", ")")
      }
    val andEventMatch = if (evtCount == 0) "" else s"""
AND (t.stream_id, t.revision) IN (
  SELECT e2.stream_id, e2.revision
  FROM $eventTable e2
  $whereEventName
)
"""
    if (chCount == 1) {
      s"s.channel = ? $andEventMatch"
    } else {
      val parms = Iterator.fill(chCount)("?")
      parms.mkString("s.channel IN (", ",", s") $andEventMatch")
    }
  }

  protected def makeStreamQuery(AND: String = ""): String = s"""
SELECT $StreamColumnsSelect
$FromJoin
WHERE t.stream_id = ? $AND
ORDER BY e.revision, e.event_idx
"""

  private val streamQueryFull = makeStreamQuery()
  def selectStreamFull(stream: ID)(thunk: (ResultSet, Columns) => Unit)(
      implicit conn: Connection): Unit = {
    conn.prepare(streamQueryFull) { ps =>
      ps.setValue(1, stream)
      executeQuery(ps) { rs =>
        thunk(rs, StreamColumnsIdx)
      }
    }
  }

  private val streamQuerySingleRevision = makeStreamQuery("AND t.revision = ?")
  def selectStreamRevision(stream: ID, revision: Revision)(thunk: (ResultSet, Columns) => Unit)(
      implicit conn: Connection): Unit = {
    conn.prepare(streamQuerySingleRevision) { ps =>
      ps.setValue(1, stream)
      ps.setInt(2, revision)
      executeQuery(ps) { rs =>
        thunk(rs, StreamColumnsIdx)
      }
    }
  }
  private val streamQueryRevisionRange = makeStreamQuery("AND t.revision BETWEEN ? AND ?")
  private val streamQueryFromRevision = makeStreamQuery("AND t.revision >= ?")
  def selectStreamRange(stream: ID, range: Range)(thunk: (ResultSet, Columns) => Unit)(
      implicit conn: Connection): Unit = {
    val bounded = range.last != Int.MaxValue
    val query = if (bounded) streamQueryRevisionRange else streamQueryFromRevision
    conn.prepare(query) { ps =>
      ps.setValue(1, stream)
      ps.setInt(2, range.head)
      if (bounded) ps.setInt(3, range.last)
      executeQuery(ps) { rs =>
        thunk(rs, StreamColumnsIdx)
      }
    }
  }
}
