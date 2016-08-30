package ulysses.jdbc

import ulysses._
import collection.{ Seq => aSeq, Map => aMap }
import java.sql.Connection
import java.sql.SQLException
import java.sql.PreparedStatement
import java.sql.ResultSet

class Dialect[ID: TypeConverter, EVT, CH: TypeConverter, SF: TypeConverter]( final val schema: String)(
    implicit final val evtCtx: EventContext[EVT, CH, SF]) {

  @inline
  final def tc[T: TypeConverter] = implicitly[TypeConverter[T]]

  def isDuplicateKeyViolation(sqlEx: SQLException): Boolean = {
    Option(sqlEx.getSQLState).exists(_ startsWith "23") ||
      Option(sqlEx.getMessage).map(_.toLowerCase).exists { msg =>
        (msg contains "duplicate") ||
          (msg contains "constraint")
      }
  }

  protected def streamTable = s"$schema.stream"
  protected def transactionTable = s"$schema.transaction"
  protected def eventTable = s"$schema.event"
  protected def metadataTable = s"$schema.metadata"

  protected def executeDDL(conn: Connection, ddl: String) {
    val stm = conn.createStatement()
    try {
      stm.execute(ddl)
    } finally {
      stm.close()
    }
  }

  def schemaDDL: String = s"CREATE SCHEMA IF NOT EXISTS $schema"
  def createSchema(conn: Connection): Unit = executeDDL(conn, schemaDDL)

  def streamTableDDL: String = s"""
    CREATE TABLE IF NOT EXISTS $streamTable (
      stream_id ${tc[ID].typeName} NOT NULL,
      channel ${tc[CH].typeName} NOT NULL,

      PRIMARY KEY (stream_id)
    )
  """
  def createStreamTable(conn: Connection): Unit = executeDDL(conn, streamTableDDL)

  def channelIndexDDL: String = s"""
    CREATE INDEX IF NOT EXISTS ${streamTable.replace(".", "_")}_channel
      ON $streamTable (channel)
  """
  def createChannelIndex(conn: Connection): Unit = executeDDL(conn, channelIndexDDL)

  def transactionTableDDL: String = s"""
    CREATE TABLE IF NOT EXISTS $transactionTable (
      stream_id ${tc[ID].typeName} NOT NULL,
      revision INT NOT NULL,
      tick BIGINT NOT NULL,

      PRIMARY KEY (stream_id, revision),
      FOREIGN KEY (stream_id)
        REFERENCES $streamTable (stream_id)
    )
  """
  def createTransactionTable(conn: Connection): Unit = executeDDL(conn, transactionTableDDL)

  def tickIndexDDL: String = s"""
    CREATE INDEX IF NOT EXISTS ${transactionTable.replace(".", "_")}_tick
      ON $transactionTable (tick)
  """
  def createTickIndex(conn: Connection): Unit = executeDDL(conn, tickIndexDDL)

  protected def eventNameType = "VARCHAR(255)"
  def eventTableDDL: String = s"""
    CREATE TABLE IF NOT EXISTS $eventTable (
      stream_id ${tc[ID].typeName} NOT NULL,
      revision INT NOT NULL,
      event_idx TINYINT NOT NULL,
      event_name $eventNameType NOT NULL,
      event_version SMALLINT NOT NULL,
      event_data ${tc[SF].typeName} NOT NULL,

      PRIMARY KEY (stream_id, revision, event_idx),
      FOREIGN KEY (stream_id, revision)
        REFERENCES $transactionTable (stream_id, revision)
    )
  """
  def createEventTable(conn: Connection): Unit = executeDDL(conn, eventTableDDL)

  def eventNameIndexDDL: String = s"""
    CREATE INDEX IF NOT EXISTS ${eventTable.replace(".", "_")}_event_name
      ON $eventTable (event_name)
  """
  def createEventNameIndex(conn: Connection): Unit = executeDDL(conn, eventNameIndexDDL)

  protected def metadataKeyType = "VARCHAR(255)"
  protected def metadataValType = "VARCHAR(65535)"

  def metadataTableDDL: String = s"""
    CREATE TABLE IF NOT EXISTS $metadataTable (
      stream_id ${tc[ID].typeName} NOT NULL,
      revision INT NOT NULL,
      metadata_key $metadataKeyType NOT NULL,
      metadata_val $metadataValType NOT NULL,

      PRIMARY KEY (stream_id, revision, metadata_key),
      FOREIGN KEY (stream_id, revision)
        REFERENCES $transactionTable (stream_id, revision)
    )
  """
  def createMetadataTable(conn: Connection): Unit = executeDDL(conn, metadataTableDDL)

  protected def setObject[T: TypeConverter](ps: PreparedStatement)(colIdx: Int, value: T) {
    ps.setObject(colIdx, tc[T].writeAs(value))
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
  def insertEvents(stream: ID, rev: Int, events: aSeq[EVT])(
    implicit conn: Connection) {
    prepareStatement(eventInsert) { ps =>
      setObject(ps)(1, stream)
      ps.setInt(2, rev)
      events.iterator.zipWithIndex.foreach {
        case (evt, idx) =>
          ps.setByte(3, idx.toByte)
          ps.setString(4, evtCtx name evt)
          ps.setShort(5, evtCtx version evt)
          setObject(ps)(6, evtCtx encode evt)
          ps.addBatch()
      }
      ps.executeUpdate()
    }
  }
  protected val metadataInsert: String = s"""
    INSERT INTO $metadataTable
      (stream_id, revision, metadata_key, metadata_val)
      VALUES (?, ?, ?, ?)
  """
  def insertMetadata(stream: ID, rev: Int, metadata: aMap[String, String])(
    implicit conn: Connection) = if (metadata.nonEmpty) {
    prepareStatement(metadataInsert) { ps =>
      setObject(ps)(1, stream)
      ps.setInt(2, rev)
      metadata.foreach {
        case (key, value) =>
          ps.setString(3, key)
          ps.setString(4, value)
          ps.addBatch()
      }
      ps.executeUpdate()
    }
  }

  private val StreamColumnsPrefixed = Seq(
    "s.channel", "t.tick", "e.revision",
    "e.event_idx", "e.event_name", "e.event_version", "e.event_data",
    "m.metadata_key", "m.metadata_val")
  private val TxnColumnsPrefixed = "s.stream_id" +: StreamColumnsPrefixed
  private def StreamColumnsSelect: String = StreamColumnsPrefixed.map { colName =>
    s"$colName AS ${colName.substring(2)}"
  }.mkString(",")
  private def TxnColumnsSelect: String = TxnColumnsPrefixed.map { colName =>
    s"$colName AS ${colName.substring(2)}"
  }.mkString(",")
  private val StreamColumnsIdx = Columns(StreamColumnsPrefixed.map(_.substring(2)).indexOf)
  private val TxnColumnsIdx = Columns(TxnColumnsPrefixed.map(_.substring(2)).indexOf)

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

  protected val txnSelect: String = s"""
    SELECT $TxnColumnsSelect
    FROM $streamTable s
    JOIN $transactionTable t
      ON t.stream_id = s.stream_id
    JOIN $eventTable e
      ON e.stream_id = t.stream_id
      AND e.revision = t.revision
    LEFT OUTER JOIN $metadataTable m
      ON m.stream_id = t.stream_id
      AND m.revision = t.revision
    WHERE stream_id = ?
    ORDER BY t.tick
  """

  //  protected val transactionQuery

  protected val maxRevisionQuery = s"SELECT MAX(revision) FROM $transactionTable WHERE stream_id = ?"
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

  protected def makeStreamQuery(AND: String = ""): String = s"""
    SELECT $StreamColumnsSelect
    FROM $streamTable s
    JOIN $transactionTable t
      ON t.stream_id = s.stream_id
    JOIN $eventTable e
      ON e.stream_id = t.stream_id
      AND e.revision = t.revision
    LEFT OUTER JOIN $metadataTable m
      ON m.stream_id = t.stream_id
      AND m.revision = t.revision
    WHERE stream_id = ? $AND
    ORDER BY e.stream_id, e.revision, e.event_idx
  """
  private val streamQueryFull = makeStreamQuery()
  private val streamQueryFromRevision = makeStreamQuery("AND revision >= ?")

  def selectStreamFull[R](stream: ID)(thunk: (ResultSet, Columns) => R)(
    implicit conn: Connection): R = {
    prepareStatement(streamQueryFull) { ps =>
      setObject(ps)(1, stream)
      executeQuery(ps) { rs =>
        thunk(rs, StreamColumnsIdx)
      }
    }
  }

}
