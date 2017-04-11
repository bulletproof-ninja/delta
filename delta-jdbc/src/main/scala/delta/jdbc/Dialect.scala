package delta.jdbc

import java.sql._

import scuff._
import delta.EventCodec

class DefaultDialect[ID: ColumnType, EVT, CH: ColumnType, SF: ColumnType](schema: String = null)
  extends Dialect[ID, EVT, CH, SF](schema.optional)

private[jdbc] object Dialect {
  def isDuplicateKeyViolation(sqlEx: SQLException): Boolean = {
    sqlEx.isInstanceOf[SQLIntegrityConstraintViolationException] ||
      Option(sqlEx.getSQLState).exists(_ startsWith "23")
  }
}

protected class Dialect[ID: ColumnType, EVT, CH: ColumnType, SF: ColumnType] protected[jdbc] (
    final val schema: Option[String]) {

  private[jdbc] def idType = implicitly[ColumnType[ID]]
  private[jdbc] def chType = implicitly[ColumnType[CH]]
  private[jdbc] def sfType = implicitly[ColumnType[SF]]

  def isDuplicateKeyViolation(sqlEx: SQLException): Boolean = Dialect.isDuplicateKeyViolation(sqlEx)

  protected def schemaPrefix = schema.map(_ + ".") getOrElse ""
  protected def streamTable = s"${schemaPrefix}es_stream"
  protected def transactionTable = s"${schemaPrefix}es_transaction"
  protected def eventTable = s"${schemaPrefix}es_event"
  protected def metadataTable = s"${schemaPrefix}es_metadata"
  protected def channelIndex = s"${streamTable.replace(".", "_")}_channel_idx"
  protected def eventNameIndex = s"${eventTable.replace(".", "_")}_event_idx"
  protected def tickIndex = s"${transactionTable.replace(".", "_")}_tick_idx"

  protected def executeDDL(conn: Connection, ddl: String) {
    val stm = conn.createStatement()
    try stm.execute(ddl) finally stm.close()
  }

  protected def schemaDDL(schema: String): String = s"CREATE SCHEMA IF NOT EXISTS $schema"
  def createSchema(conn: Connection): Unit = schema.foreach(schema => executeDDL(conn, schemaDDL(schema)))

  protected def streamTableDDL: String = s"""
    CREATE TABLE IF NOT EXISTS $streamTable (
      stream_id ${idType.typeName} NOT NULL,
      channel ${chType.typeName} NOT NULL,

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

  protected def eventNameType = "VARCHAR(255)"
  protected def eventTableDDL: String = s"""
    CREATE TABLE IF NOT EXISTS $eventTable (
      stream_id ${idType.typeName} NOT NULL,
      revision INT NOT NULL,
      event_idx TINYINT NOT NULL,
      event_name $eventNameType NOT NULL,
      event_version TINYINT NOT NULL,
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

  protected def setObject[T: ColumnType](ps: PreparedStatement)(colIdx: Int, value: T) {
    ps.setObject(colIdx, implicitly[ColumnType[T]].writeAs(value))
  }
  private def prepareStatement[R](sql: String)(thunk: PreparedStatement => R)(
    implicit conn: Connection): R = {
    val ps = conn.prepareStatement(sql)
    try thunk(ps) finally ps.close()
  }
  private def executeQuery[R](ps: PreparedStatement)(thunk: ResultSet => R): R = {
    val rs = ps.executeQuery()
    try thunk(rs) finally rs.close()
  }
  protected val streamInsert: String = s"""
    INSERT INTO $streamTable
      (stream_id, channel)
      VALUES (?, ?)
  """
  def insertStream(stream: ID, channel: CH)(
    implicit conn: Connection) {
    prepareStatement(streamInsert) { ps =>
      setObject(ps)(1, stream)
      setObject(ps)(2, channel)
      ps.executeUpdate()
    }
  }
  protected val transactionInsert: String = s"""
      INSERT INTO $transactionTable
        (stream_id, revision, tick)
        VALUES (?, ?, ?)
  """
  def insertTransaction(stream: ID, rev: Int, tick: Long)(
    implicit conn: Connection) {
    prepareStatement(transactionInsert) { ps =>
      setObject(ps)(1, stream)
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
  def insertEvents(stream: ID, rev: Int, events: List[EVT])(
    implicit conn: Connection, codec: EventCodec[EVT, SF]) {
    prepareStatement(eventInsert) { ps =>
      val isBatch = events.tail.nonEmpty
      setObject(ps)(1, stream)
      ps.setInt(2, rev)
      events.iterator.zipWithIndex.foreach {
        case (evt, idx) =>
          ps.setByte(3, idx.toByte)
          ps.setString(4, codec name evt)
          ps.setByte(5, codec version evt)
          setObject(ps)(6, codec encode evt)
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
  def insertMetadata(stream: ID, rev: Int, metadata: Map[String, String])(
    implicit conn: Connection) = if (metadata.nonEmpty) {
    prepareStatement(metadataInsert) { ps =>
      val isBatch = metadata.size > 1
      setObject(ps)(1, stream)
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
  private val TxnColumnsPrefixed = "s.stream_id" +: StreamColumnsPrefixed
  private def StreamColumnsSelect: String = StreamColumnsPrefixed.map { colName =>
    s"$colName AS ${colName.substring(2)}"
  }.mkString(",")
  private def TxnColumnsSelect: String = TxnColumnsPrefixed.map { colName =>
    s"$colName AS ${colName.substring(2)}"
  }.mkString(",")
  private val StreamColumnsIdx = Columns(StreamColumnsPrefixed.map(_.substring(2)).indexOf(_) + 1)
  private val TxnColumnsIdx = Columns(TxnColumnsPrefixed.map(_.substring(2)).indexOf(_) + 1)

  private[jdbc] case class Columns(
    stream_id: Int, revision: Int,
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
    prepareStatement(maxRevisionQuery) { ps =>
      ps.setFetchSize(1)
      setObject(ps)(1, stream)
      executeQuery(ps) { rs =>
        if (rs.next) Some(rs.getInt(1))
        else None
      }
    }
  }

  protected val maxTickQuery = s"SELECT MAX(tick) FROM $transactionTable"
  def selectMaxTick(implicit conn: Connection): Option[Long] = {
    prepareStatement(maxTickQuery) { ps =>
      ps.setFetchSize(1)
      executeQuery(ps) { rs =>
        if (rs.next) Some(rs.getLong(1))
        else None
      }
    }
  }

  protected def FromJoin = s"""
    FROM $streamTable s
    JOIN $transactionTable t
      ON t.stream_id = s.stream_id
    JOIN $eventTable e
      ON e.stream_id = t.stream_id
      AND e.revision = t.revision
    LEFT OUTER JOIN $metadataTable m
      ON m.stream_id = t.stream_id
      AND m.revision = t.revision
  """
  protected def makeTxnQuery(WHERE: String = ""): String = s"""
    SELECT $TxnColumnsSelect
    $FromJoin
    $WHERE
    ORDER BY e.stream_id, e.revision, e.event_idx
  """

  def selectTransactionsByChannels(channels: Set[CH], sinceTick: Long = Long.MinValue)(
    thunk: (ResultSet, Columns) => Unit)(
      implicit conn: Connection): Unit = {
    val tickBound = sinceTick != Long.MinValue
    val prefix = if (tickBound) {
      s"WHERE t.tick >= ? AND"
    } else "WHERE"
    val WHERE = s"$prefix ${makeWHEREByChannelsOrEvents(channels.size)}"
    val query = makeTxnQuery(WHERE)
    prepareStatement(query) { ps =>
      val colIdx = Iterator.from(1)
      if (tickBound) ps.setLong(colIdx.next, sinceTick)
      channels foreach { channel =>
        setObject(ps)(colIdx.next, channel)
      }
      executeQuery(ps) { rs =>
        thunk(rs, TxnColumnsIdx)
      }
    }
  }
  def selectTransactionsByEvents(eventsByChannel: Map[CH, Set[Class[_ <: EVT]]], sinceTick: Long = Long.MinValue)(
    thunk: (ResultSet, Columns) => Unit)(
      implicit conn: Connection, codec: EventCodec[EVT, SF]): Unit = {
    val tickBound = sinceTick != Long.MinValue
    val prefix = if (tickBound) {
      s"WHERE t.tick >= ? AND"
    } else "WHERE"
    val channels = eventsByChannel.keys
    val events = eventsByChannel.values.flatten
    val WHERE = s"$prefix ${makeWHEREByChannelsOrEvents(channels.size, events.size)}"
    val query = makeTxnQuery(WHERE)
    prepareStatement(query) { ps =>
      val colIdx = Iterator.from(1)
      if (tickBound) ps.setLong(colIdx.next, sinceTick)
      channels foreach { channel =>
        setObject(ps)(colIdx.next, channel)
      }
      events foreach { evt =>
        ps.setString(colIdx.next, codec.name(evt))
      }
      executeQuery(ps) { rs =>
        thunk(rs, TxnColumnsIdx)
      }
    }
  }
  def selectTransactions(sinceTick: Long = Long.MinValue)(
    thunk: (ResultSet, Columns) => Unit)(
      implicit conn: Connection): Unit = {
    val tickBound = sinceTick != Long.MinValue
    val query = if (tickBound) {
      makeTxnQuery(s"WHERE t.tick >= ?")
    } else {
      makeTxnQuery()
    }
    prepareStatement(query) { ps =>
      if (tickBound) ps.setLong(1, sinceTick)
      executeQuery(ps) { rs =>
        thunk(rs, TxnColumnsIdx)
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
    val andEventMatch = if (evtCount == 0) "" else s"""AND
      (t.stream_id, t.revision) IN (
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
    prepareStatement(streamQueryFull) { ps =>
      setObject(ps)(1, stream)
      executeQuery(ps) { rs =>
        thunk(rs, StreamColumnsIdx)
      }
    }
  }

  private val streamQuerySingleRevision = makeStreamQuery("AND t.revision = ?")
  def selectStreamRevision(stream: ID, revision: Int)(thunk: (ResultSet, Columns) => Unit)(
    implicit conn: Connection): Unit = {
    prepareStatement(streamQuerySingleRevision) { ps =>
      setObject(ps)(1, stream)
      ps.setInt(2, revision)
      executeQuery(ps) { rs =>
        thunk(rs, StreamColumnsIdx)
      }
    }
  }
  private val streamQueryRevisionRange = makeStreamQuery("AND t.revision BETWEEN ? AND ?")
  def selectStreamRange(stream: ID, range: Range)(thunk: (ResultSet, Columns) => Unit)(
    implicit conn: Connection): Unit = {
    prepareStatement(streamQueryRevisionRange) { ps =>
      setObject(ps)(1, stream)
      ps.setInt(2, range.head)
      ps.setInt(3, range.last)
      executeQuery(ps) { rs =>
        thunk(rs, StreamColumnsIdx)
      }
    }
  }
}
