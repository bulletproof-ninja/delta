package delta.jdbc

import java.sql.{ Connection, SQLException }

import scala.collection.compat._
import scala.collection.immutable.HashMap
import scala.concurrent.Future
import scala.util.Try

import delta._
import delta.process._

import scuff.jdbc.AsyncConnectionSource

/**
 * Keep history of all snapshots generated,
 * to ensure a complete audit trail.
 */
class JdbcStreamProcessHistory[ID: ColumnType, S: ColumnType, U](
    protected val connectionSource: AsyncConnectionSource,
    version: Short, withTimestamp: UpdateTimestamp,
    pkColName: String, table: String,
    schema: Option[String] = None)(
    implicit
    protected val updateCodec: UpdateCodec[S, U])
  extends AbstractJdbcStore(Some(version), schema.map(_ concat s".$table") getOrElse table.trim, schema)
  with StreamProcessStore[ID, S, U]
  with BlockingCASWrites[ID, S, U, Connection] {

  private def insertTransaction(withData: Boolean): String = s"""
INSERT INTO $tableRef
(version, $pkColName, tick, revision, ${withTimestamp.columnName}, data)
VALUES($version, ?, ?, ?, ${withTimestamp.sqlFunction}, ${if (withData) "?" else "NULL"})"""

  protected val insertTransactionSQL = insertTransaction(withData = false)
  protected val insertSnapshotSQL = insertTransaction(withData = true)

  protected val updateTransactionRevTickSQL: String = s"""
UPDATE $tableRef
SET tick = ?, revision = ?
WHERE version = $version
AND $pkColName = ?
AND tick <= ?
AND revision <= ?
AND data is NULL
"""

  protected val updateSnapshotSQL: String = s"""
UPDATE $tableRef
SET ${withTimestamp.columnName} = ${withTimestamp.sqlFunction}, data = ?, revision = ?
WHERE version = $version
AND $pkColName = ?
AND tick = ?
AND revision = ?
"""

  protected def createTableDDL = s"""
CREATE TABLE IF NOT EXISTS $tableRef (
  version SMALLINT NOT NULL,
  $pkColName ${implicitly[ColumnType[ID]].typeName} NOT NULL,
  tick BIGINT NOT NULL,
  revision INT NOT NULL,
  ${withTimestamp.columnName} ${withTimestamp.sqlType} NOT NULL,
  data ${implicitly[ColumnType[S]].typeName} NULL,

  PRIMARY KEY (version, $pkColName, tick)
)"""

  protected val selectSnapshotSQL = s"""
SELECT t.revision, t.tick, s.data
FROM $tableRef t
JOIN $tableRef s
ON t.version = s.version
AND t.$pkColName = s.$pkColName
AND s.tick = (
  SELECT MAX(tick) FROM $tableRef ms
  WHERE ms.version = t.version
    AND ms.$pkColName = s.$pkColName
    AND ms.data IS NOT NULL)
WHERE t.version = $version
AND t.$pkColName = ?
AND t.tick = (
  SELECT MAX(tick)
  FROM $tableRef mt
  WHERE mt.version = t.version
    AND mt.$pkColName = t.$pkColName)
"""

  protected def createTable(conn: Connection): Unit = createTable(conn, createTableDDL)
  protected def createTickIndex(conn: Connection): Unit = createIndex(conn, createTickIndexDDL)

  protected def readForUpdate[R](key: ID)(thunk: (Connection, Option[Snapshot]) => R): Future[R] = {
    connectionSource.asyncUpdate { conn =>
      val existing = getOne(conn, key)
      thunk(conn, existing)
    }
  }

  def read(key: ID): Future[Option[Snapshot]] =
    futureQuery {
      getOne(_, key)
    }
  def readBatch(keys: Iterable[ID]): Future[Map[ID, Snapshot]] =
    futureQuery {
      getAll(_, keys)
    }

  private def getOne(conn: Connection, key: ID): Option[Snapshot] = getAll(conn, key :: Nil) get key
  private def getAll(conn: Connection, keys: Iterable[ID]): Map[ID, Snapshot] =
    conn.prepare(selectSnapshotSQL) { ps =>
      keys.foldLeft(HashMap.empty[ID, Snapshot]) {
        case (map, key) =>
          ps.setValue(1, key)
          val rs = ps.executeQuery()
          try {
            if (rs.next) {
              val revision = rs.getInt(1)
              val tick = rs.getLong(2)
              val data = rs.getValue[S](3)
              if (rs.wasNull) map
              else map.updated(key, new Snapshot(data, revision, tick))
            } else map
          } finally Try(rs.close)
      }
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
    if (snapshots.isEmpty) Future.unit
    else futureUpdate { conn =>
      // Optimistically insert, since writing large batches is mostly done when processing streams from scratch
      // NOTE: Some JDBC drivers may fail on the entire batch, if a single row fails
      val failed = insertSnapshots(conn, snapshots).toSet
      if (failed.nonEmpty) {
        snapshots.view.filterKeys(failed).foreach {
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
      conn.prepare(insertTransactionSQL) { ps =>
        val keys = revisions.toSeq.map {
          case (key, (revision, tick)) =>
            ps.setValue(1, key)
            ps.setLong(2, tick)
            ps.setInt(3, revision)
            if (isBatch) ps.addBatch()
            key
        }
        try {
          if (isBatch) executeBatch(ps, keys)
          else if (ps.executeUpdate() == 1) Nil
          else keys
        } catch {
          case sqlEx: SQLException if isDuplicateKeyViolation(sqlEx) =>
            revisions.keys
        }
      }
    }

  protected def updateTransaction(conn: Connection, key: ID, rev: Revision, tick: Tick): Boolean =
    conn.prepare(updateTransactionRevTickSQL) { ps =>
      // SET
      ps.setLong(1, tick)
      ps.setInt(2, rev)
      // WHERE
      ps.setValue(3, key)
      ps.setLong(4, tick)
      ps.setInt(5, rev)
      ps.executeUpdate() == 1
    }

  /** Insert snapshots, return any failed keys. */
  protected def insertSnapshots(conn: Connection, snapshots: collection.Map[ID, Snapshot]): Iterable[ID] =
    if (snapshots.isEmpty) Nil
    else conn.prepare(insertSnapshotSQL) { ps =>
      val isBatch = snapshots.tail.nonEmpty
      val keys = snapshots.toSeq.map {
        case (key, Snapshot(data, revision, tick)) =>
          ps.setValue(1, key)
          ps.setLong(2, tick)
          ps.setInt(3, revision)
          ps.setValue(4, data)
          if (isBatch) ps.addBatch()
          key
      }
      try {
        if (isBatch) executeBatch(ps, keys)
        else if (ps.executeUpdate() == 1) Nil
        else keys
      } catch {
        case sqlEx: SQLException if isDuplicateKeyViolation(sqlEx) =>
          snapshots.keys
      }
    }

  /**
   * Calling this should be exceedingly unlikely,
   * so long as ticks are generated properly.
   * Basically only when there's a tick collision,
   * which is only possible on join data.
   */
  protected def updateSnapshot(conn: Connection, key: ID, snapshot: Snapshot): Boolean =
    conn.prepare(updateSnapshotSQL) { ps =>
      // SET
      ps.setValue(1, snapshot.state)
      ps.setInt(2, snapshot.revision)
      // WHERE
      ps.setValue(3, key)
      ps.setLong(4, snapshot.tick)
      ps.setInt(5, snapshot.revision)
      ps.executeUpdate() == 1
    }

  protected def refreshKey(conn: Connection)(key: ID, revision: Revision, tick: Tick): Unit =
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

  def refresh(key: ID, revision: Revision, tick: Tick): Future[Unit] =
    refreshBatch(Map(key -> (revision -> tick)))

  def refreshBatch(revisions: collection.Map[ID, (Int, Long)]): Future[Unit] =
    if (revisions.isEmpty) Future.unit
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
