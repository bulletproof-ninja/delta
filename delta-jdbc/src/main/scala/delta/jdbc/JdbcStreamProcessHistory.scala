package delta.jdbc

import java.sql.{ Connection, SQLException }

import scala.collection.Map
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

import delta.Snapshot
import delta.util.{ Exceptions, StreamProcessStore }
import scuff.ScuffString
import scuff.jdbc.ConnectionProvider

/**
  * Keep history of all snapshots generated,
  * to ensure a complete audit trail.
  */
class JdbcStreamProcessHistory[ID: ColumnType, D: ColumnType](
    jdbcCtx: ExecutionContext,
    val version: Short,
    table: String, schema: Option[String] = None)
  extends AbstractStore()(jdbcCtx)
  with StreamProcessStore[ID, D] {
  cp: ConnectionProvider =>

  def this(jdbcCtx: ExecutionContext, version: Short, table: String, schema: String) =
    this(jdbcCtx, version, table, schema.optional)

  protected implicit def jdbcExeCtx = jdbcCtx

  /** Ensure table. */
  def ensureTable(): this.type =
    forUpdate { conn =>
      createTable(conn)
      createTickIndex(conn)
      this
    }

  private[this] val schemaRef = schema.map(_ + ".") getOrElse ""
  protected val tableRef: String = schemaRef concat table

  private def insertTransaction(withData: Boolean): String = s"""
INSERT INTO $tableRef
(version, $idColName, tick, revision, $timestampColName, data)
VALUES($version, ?, ?, ?, """ + (if (withData) timestampNowFunction + ", ?)" else "NULL, NULL)")
  protected val insertTransactionSQL = insertTransaction(withData = false)
  protected val insertSnapshotSQL = insertTransaction(withData = true)

  protected val updateTransactionRevTickSQL: String = s"""
UPDATE $tableRef
SET tick = ?, revision = ?
WHERE version = $version
AND $idColName = ?
AND tick <= ?
AND revision <= ?
AND data is NULL
""".trim

  protected val updateSnapshotSQL: String = s"""
UPDATE $tableRef
SET $timestampColName = $timestampNowFunction, data = ?, revision = ?
WHERE version = $version
AND $idColName = ?
AND tick = ?
AND revision = ?
""".trim

  protected def idColName: String = "id"
  protected def timestampColName: String = "since"
  protected def timestampColType: String = "TIMESTAMP"
  protected def timestampNowFunction: String = "NOW()"

  protected def createTableDDL = s"""
CREATE TABLE IF NOT EXISTS $tableRef (
  version SMALLINT NOT NULL,
  $idColName ${implicitly[ColumnType[ID]].typeName} NOT NULL,
  tick BIGINT NOT NULL,
  revision INT NOT NULL,
  $timestampColName $timestampColType NULL,
  data ${implicitly[ColumnType[D]].typeName} NULL,

  PRIMARY KEY (version, $idColName, tick)
)""".trim

  protected def tickIndexName = tableRef.replace(".", "_") concat "_tick"
  protected def createTickIndexDDL = s"""
CREATE INDEX IF NOT EXISTS $tickIndexName
  ON $tableRef (version, tick)
""".trim

  protected val selectSnapshotSQL = s"""
SELECT t.revision, t.tick, s.data
FROM $tableRef t
JOIN $tableRef s
ON t.version = s.version
AND t.$idColName = s.$idColName
AND s.tick = (
  SELECT MAX(tick) FROM $tableRef ms
  WHERE ms.version = t.version
    AND ms.$idColName = s.$idColName
    AND ms.data IS NOT NULL)
WHERE t.version = $version
AND t.$idColName = ?
AND t.tick = (
  SELECT MAX(tick)
  FROM $tableRef mt
  WHERE mt.version = t.version
    AND mt.$idColName = t.$idColName)
""".trim

  protected def createTable(conn: Connection): Unit = createTable(conn, createTableDDL)
  protected def createTickIndex(conn: Connection): Unit = createIndex(conn, createTickIndexDDL)

  def maxTick: Future[Option[Long]] =
    futureQuery { conn =>
      maxTick(tableRef, version)(conn)
    }

  def read(key: ID): Future[Option[Snapshot]] = readBatch(List(key)).map(_.get(key))
  def readBatch(keys: Iterable[ID]): Future[Map[ID, Snapshot]] =
    futureQuery { conn =>
      getAll(conn, keys)
    }

  private def getAll(conn: Connection, keys: Iterable[ID], map: Map[ID, Snapshot] = Map.empty): Map[ID, Snapshot] = {
    val ps = conn prepareStatement selectSnapshotSQL
    try {
      keys.foldLeft(map) {
        case (map, key) =>
          ps.setValue(1, key)
          val rs = ps.executeQuery()
          try {
            if (rs.next) {
              val revision = rs.getInt(1)
              val tick = rs.getLong(2)
              val data = rs.getValue[D](3)
              if (rs.wasNull) map
              else map.updated(key, new Snapshot(data, revision, tick))
            } else map
          } finally Try(rs.close)
      }
    } finally Try(ps.close)
  }

  def write(key: ID, snapshot: Snapshot): Future[Unit] = {
    val existing = conditionalWrite(key) {
      case (conn, None) =>
        insertSnapshots(conn, Map((key, snapshot))).isEmpty
      case (conn, Some(existing)) if existing.revision <= snapshot.revision && existing.tick < snapshot.tick =>
        insertSnapshots(conn, Map((key, snapshot))).isEmpty
      case (conn, Some(existing)) if existing.revision <= snapshot.revision && existing.tick == snapshot.tick =>
        updateSnapshot(conn, key, snapshot)
      case (_, Some(existing)) =>
        throw Exceptions.writeOlder(key, existing, snapshot)
    }
    existing.map {
      case None => ()
      case Some(existing) =>
        throw Exceptions.writeOlder(key, existing, snapshot)
    }
  }
  def writeBatch(snapshots: Map[ID, Snapshot]): Future[Unit] =
    if (snapshots.isEmpty) Future successful Unit
    else futureUpdate { conn =>
      val failed = insertSnapshots(conn, snapshots).toSet
      if (failed.nonEmpty) {
        snapshots.filterKeys(failed).foreach {
          case (key, snapshot) =>
            if (!updateSnapshot(conn, key, snapshot)) {
              sys.error(s"Failed to insert and update $snapshot")
            }
        }
      }
    }

  protected def insertRevisions(conn: Connection, revisions: Map[ID, (Int, Long)]): Iterable[ID] =
    if (revisions.isEmpty) Nil else {
      val isBatch = revisions.tail.nonEmpty
      val ps = conn.prepareStatement(insertTransactionSQL)
      try {
        val keys = revisions.toSeq.map {
          case (key, (revision, tick)) =>
            ps.setValue(1, key)
            ps.setLong(2, tick)
            ps.setInt(3, revision)
            if (isBatch) ps.addBatch()
            key
        }
        if (isBatch) executeBatch(ps, keys)
        else if (ps.executeUpdate() == 1) Seq.empty
        else keys
      } catch {
        case sqlEx: SQLException if isDuplicateKeyViolation(sqlEx) =>
          revisions.keys
      } finally Try(ps.close)
    }

  protected def updateTransaction(conn: Connection, key: ID, rev: Int, tick: Long): Boolean = {
    val ps = conn.prepareStatement(updateTransactionRevTickSQL)
    try {
      // SET
      ps.setLong(1, tick)
      ps.setInt(2, rev)
      // WHERE
      ps.setValue(3, key)
      ps.setLong(4, tick)
      ps.setInt(5, rev)
      ps.executeUpdate() == 1
    } finally Try(ps.close)
  }

  /** Insert snapshots, return any failed keys. */
  protected def insertSnapshots(conn: Connection, snapshots: Map[ID, Snapshot]): Iterable[ID] =
    if (snapshots.isEmpty) Nil else {
      val isBatch = snapshots.tail.nonEmpty
      val ps = conn.prepareStatement(insertSnapshotSQL)
      try {
        val keys = snapshots.toSeq.map {
          case (key, Snapshot(data, revision, tick)) =>
            ps.setValue(1, key)
            ps.setLong(2, tick)
            ps.setInt(3, revision)
            ps.setValue(4, data)
            if (isBatch) ps.addBatch()
            key
        }
        if (isBatch) executeBatch(ps, keys)
        else if (ps.executeUpdate() == 1) Nil
        else keys
      } catch {
        case sqlEx: SQLException if isDuplicateKeyViolation(sqlEx) =>
          snapshots.keys
      } finally Try(ps.close)
    }

  /**
    * Calling this should be exceedingly unlikely,
    * so long as ticks are generated properly.
    * Basically only when there's a tick collision,
    * which is only possible on collateral data.
    */
  protected def updateSnapshot(conn: Connection, key: ID, snapshot: Snapshot): Boolean = {
    val ps = conn.prepareStatement(updateSnapshotSQL)
    try {
      // SET
      ps.setValue(1, snapshot.content)
      ps.setInt(2, snapshot.revision)
      // WHERE
      ps.setValue(3, key)
      ps.setLong(4, snapshot.tick)
      ps.setInt(5, snapshot.revision)
      ps.executeUpdate() == 1
    } finally Try(ps.close)
  }

  def refresh(key: ID, revision: Int, tick: Long): Future[Unit] =
    refreshBatch(Map(key -> (revision -> tick)))

  def refreshBatch(revisions: Map[ID, (Int, Long)]): Future[Unit] =
    if (revisions.isEmpty) Future successful Unit
    else futureUpdate { conn =>
      val notUpdated = revisions.collect {
        case entry @ (key, (rev, tick)) if !updateTransaction(conn, key, rev, tick) =>
          entry
      }
      insertRevisions(conn, notUpdated)
      // Anything failed here, is discarded as too old.
      // No data is lost, so no point throwing exception
    }

  private def conditionalWrite(key: ID)(write: (Connection, Option[Snapshot]) => Boolean): Future[Option[Snapshot]] =
    futureUpdate { conn =>
      val configuredIsolation = conn.getTransactionIsolation
      conn setTransactionIsolation Connection.TRANSACTION_SERIALIZABLE
      try {
        val existing: Option[Snapshot] = getAll(conn, List(key)).get(key)
        if (write(conn, existing)) None
        else {
          conn.rollback()
          existing
        }
      } finally {
        Try(conn.commit)
        conn setTransactionIsolation configuredIsolation
      }
    }

  protected def writeIfAbsent(key: ID, snapshot: Snapshot): Future[Option[Snapshot]] =
    conditionalWrite(key) {
      case (conn, None) =>
        insertSnapshots(conn, Map(key -> snapshot)).isEmpty
      case _ =>
        false
    }

  protected def writeReplacement(key: ID, oldSnapshot: Snapshot, newSnapshot: Snapshot): Future[Option[Snapshot]] = {
    if (oldSnapshot.tick == newSnapshot.tick) conditionalWrite(key) {
      case (conn, Some(existing)) if existing == oldSnapshot =>
        updateSnapshot(conn, key, newSnapshot)
      case (_, None) =>
        sys.error(s"Tried to replace unknown snapshot: $oldSnapshot")
      case _ =>
        false
    }
    else conditionalWrite(key) {
      case (conn, Some(existing)) if existing == oldSnapshot =>
        insertSnapshots(conn, Map(key -> newSnapshot)).isEmpty
      case (_, None) => sys.error(s"Tried to replace unknown snapshot: $oldSnapshot")
      case _ =>
        false
    }
  }

}
