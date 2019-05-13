package delta.jdbc

import java.sql.{ Connection, SQLException }

import scala.collection.immutable.HashMap
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

import delta.Snapshot
import delta.process.{ BlockingCASWrites, Exceptions, StreamProcessStore }
import scuff._
import scuff.jdbc.ConnectionSource

/**
 * Keep history of all snapshots generated,
 * to ensure a complete audit trail.
 */
class JdbcStreamProcessHistory[ID: ColumnType, D: ColumnType](
    jdbcCtx: ExecutionContext,
    cs: ConnectionSource,
    version: Short,
    table: String, schema: Option[String] = None)
  extends AbstractStore(cs, Some(version), table, schema)(jdbcCtx)
  with StreamProcessStore[ID, D] with BlockingCASWrites[ID, D, Connection] {

  def this(
      jdbcCtx: ExecutionContext,
      cs: ConnectionSource,
      version: Short, table: String, schema: String) =
    this(jdbcCtx, cs, version, table, schema.optional)

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
"""

  protected val updateSnapshotSQL: String = s"""
UPDATE $tableRef
SET $timestampColName = $timestampNowFunction, data = ?, revision = ?
WHERE version = $version
AND $idColName = ?
AND tick = ?
AND revision = ?
"""

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
)"""

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
"""

  protected def createTable(conn: Connection): Unit = createTable(conn, createTableDDL)
  protected def createTickIndex(conn: Connection): Unit = createIndex(conn, createTickIndexDDL)

  protected def readForUpdate[R](key: ID)(thunk: (Connection, Option[Snapshot]) => R): R = {
    cs.forUpdate { conn =>
      val existing = getOne(conn, key)
      thunk(conn, existing)
    }
  }

  def read(key: ID): Future[Option[Snapshot]] = readBatch(List(key)).map(_.get(key))
  def readBatch(keys: Iterable[ID]): Future[Map[ID, Snapshot]] =
    futureQuery { conn =>
      getAll(conn, keys)
    }

  private def getOne(conn: Connection, key: ID): Option[Snapshot] = getAll(conn, key :: Nil).get(key)
  private def getAll(conn: Connection, keys: Iterable[ID]): Map[ID, Snapshot] = {
    val ps = conn prepareStatement selectSnapshotSQL
    try {
      keys.foldLeft(HashMap.empty[ID, Snapshot]) {
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

  def write(key: ID, snapshot: Snapshot): Future[Unit] = futureUpdate { conn =>
    val existing = conditionalWrite(conn, key) {
      case None =>
        insertSnapshots(conn, Map((key, snapshot))).isEmpty
      case Some(existing) if existing.revision <= snapshot.revision && existing.tick < snapshot.tick =>
        insertSnapshots(conn, Map((key, snapshot))).isEmpty
      case Some(existing) if existing.revision <= snapshot.revision && existing.tick == snapshot.tick =>
        updateSnapshot(conn, key, snapshot)
      case Some(existing) =>
        throw Exceptions.writeOlder(key, existing, snapshot)
    }
    existing match {
      case None => ()
      case Some(existing) =>
        throw Exceptions.writeOlder(key, existing, snapshot)
    }
  }
  def writeBatch(snapshots: collection.Map[ID, Snapshot]): Future[Unit] =
    if (snapshots.isEmpty) Future successful (())
    else futureUpdate { conn =>
      // Optimistically insert, since writing large batches is mostly done when processing streams from scratch
      // NOTE: Some JDBC drivers may fail on the entire batch, if a single row fails
      val failed = insertSnapshots(conn, snapshots).toSet
      if (failed.nonEmpty) {
        snapshots.filterKeys(failed).foreach {
          case (key, snapshot) =>
            if (!updateSnapshot(conn, key, snapshot)) {
              // To deal with drivers failing on entire batch, we once again try to insert, this time single row
              val failed = insertSnapshots(conn, Map(key -> snapshot))
              if (failed.nonEmpty) {
                sys.error(s"Failed to insert and update $key: $snapshot")
              }
            }
        }
      }
    }

  protected def insertRevisions(conn: Connection, revisions: collection.Map[ID, (Int, Long)]): Iterable[ID] =
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
        else if (ps.executeUpdate() == 1) Nil
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
  protected def insertSnapshots(conn: Connection, snapshots: collection.Map[ID, Snapshot]): Iterable[ID] =
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
   * which is only possible on join data.
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

  protected def refreshKey(conn: Connection)(key: ID, revision: Int, tick: Long): Unit =
    refreshAll(conn, Map(key -> (revision -> tick)))

  private def refreshAll(conn: Connection, revisions: collection.Map[ID, (Int, Long)]): Unit = {
    val notUpdated = revisions.collect {
      case entry @ (key, (rev, tick)) if !updateTransaction(conn, key, rev, tick) =>
        entry
    }
    insertRevisions(conn, notUpdated)
    // Anything failed here, is discarded as too old.
    // No data is lost, so no point throwing exception
  }

  def refresh(key: ID, revision: Int, tick: Long): Future[Unit] =
    refreshBatch(Map(key -> (revision -> tick)))

  def refreshBatch(revisions: collection.Map[ID, (Int, Long)]): Future[Unit] =
    if (revisions.isEmpty) Future successful (())
    else futureUpdate(refreshAll(_, revisions))

  private def conditionalWrite(conn: Connection, key: ID)(write: Option[Snapshot] => Boolean): Option[Snapshot] = {
    val existing: Option[Snapshot] = getOne(conn, key)
    if (write(existing)) None
    else {
      conn.rollback()
      existing
    }
  }

  protected def writeIfAbsent(conn: Connection)(key: ID, snapshot: Snapshot): Option[Snapshot] =
    conditionalWrite(conn, key) {
      case None =>
        insertSnapshots(conn, Map(key -> snapshot)).isEmpty
      case _ =>
        false
    }

  protected def writeReplacement(conn: Connection)(key: ID, oldSnapshot: Snapshot, newSnapshot: Snapshot): Option[Snapshot] = {
    if (oldSnapshot.tick == newSnapshot.tick) conditionalWrite(conn, key) {
      case Some(existing) if existing == oldSnapshot =>
        updateSnapshot(conn, key, newSnapshot)
      case None =>
        sys.error(s"Tried to replace unknown snapshot: $oldSnapshot")
      case _ =>
        false
    }
    else conditionalWrite(conn, key) {
      case Some(existing) if existing == oldSnapshot =>
        insertSnapshots(conn, Map(key -> newSnapshot)).isEmpty
      case None => sys.error(s"Tried to replace unknown snapshot: $oldSnapshot")
      case _ =>
        false
    }
  }

}
