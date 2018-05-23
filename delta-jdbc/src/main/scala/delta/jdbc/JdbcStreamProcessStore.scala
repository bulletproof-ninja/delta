package delta.jdbc

import java.sql._

import scala.collection.Map
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

import delta.util.{ StreamProcessStore, BlockingCASWrites }
import scuff._
import scuff.jdbc.ConnectionProvider
import delta.util.Exceptions

abstract class AbstractJdbcStreamProcessStore[K, D: ColumnType] protected (
    val version: Short,
    table: String, schema: Option[String])(
    implicit jdbcCtx: ExecutionContext)
  extends AbstractStore
  with StreamProcessStore[K, D] with BlockingCASWrites[K, D, Connection] {
  cp: ConnectionProvider =>

  case class ColumnDef(name: String, colTypeName: String)

  def ensureTable(): this.type =
    forUpdate { conn =>
      createTable(conn)
      createIndex(conn)
      this
    }

  def tickWatermark: Option[Long] =
    forQuery { conn =>
      maxTick(tableRef, version)(conn)
    }

  private[this] val schemaRef = schema.map(_ concat ".") getOrElse ""
  protected def tableRef: String = schemaRef concat table
  protected def pkColumns: Seq[ColumnDef]

  protected def createTableDDL = {
    val pkColumnDefs = pkColumns.map {
      case ColumnDef(name, colTypeName) => s"$name $colTypeName NOT NULL"
    }.mkString(",")

    val pkNames = pkColumns.map(_.name).mkString(",")

    s"""
CREATE TABLE IF NOT EXISTS $tableRef (
  version SMALLINT NOT NULL,
  $pkColumnDefs,
  data ${implicitly[ColumnType[D]].typeName},
  revision INT NOT NULL,
  tick BIGINT NOT NULL,

  PRIMARY KEY (version, $pkNames)
)"""
  }

  protected def tickIndexName = tableRef.replace(".", "_") concat "_tick"
  protected def createTickIndexDDL = s"""
    CREATE INDEX IF NOT EXISTS $tickIndexName
      ON $tableRef (version, tick)
  """.trim

  protected def selectOneSQL = _selectOneSQL
  private[this] val _selectOneSQL = {
    val pkNamesEqual = pkColumns.map { cd =>
      s"${cd.name} = ?"
    }.mkString(" AND ")
    s"""
SELECT data, revision, tick
FROM $tableRef
WHERE version = $version
AND $pkNamesEqual
    """.trim
  }
  protected def insertSnapshotSQL = _insertSnapshotSQL
  private[this] val _insertSnapshotSQL = {
    val pkNames = pkColumns.map(_.name).mkString(",")
    val pkQs = Iterable.fill(pkColumns.size)("?").mkString(",")
    s"""
INSERT INTO $tableRef
(version, data, revision, tick, $pkNames)
VALUES ($version, ?,?,?,$pkQs)
    """.trim
  }
  protected def setParmsOnInsert(ps: PreparedStatement, key: K, snapshot: Snapshot): Int = {
    val offset = setSnapshot(ps, snapshot)
    setKeyParms(ps, key, offset)
  }

  protected def replaceSnapshotSQL = _replaceSnapshotSQL
  private[this] val _replaceSnapshotSQL = updateSnapshotSQL(exact = true)

  protected def updateSnapshotDefensiveSQL = _updateSnapshotDefensiveSQL
  private[this] val _updateSnapshotDefensiveSQL = updateSnapshotSQL(exact = false)

  protected def updateSnapshotSQL(exact: Boolean): String = {
    val matches = if (exact) "=" else "<="
    val pkNamesEqual = pkColumns.map { cd =>
      s"${cd.name} = ?"
    }.mkString(" AND ")
    s"""
UPDATE $tableRef
SET data = ?, revision = ?, tick = ?
WHERE version = $version
AND $pkNamesEqual
AND revision $matches ? AND tick $matches ?
    """.trim
  }

  protected def refreshRevTickDefensiveSQL = _refreshRevTickDefensiveSQL
  private[this] val _refreshRevTickDefensiveSQL = {
    val pkNamesEqual = pkColumns.map { cd =>
      s"${cd.name} = ?"
    }.mkString(" AND ")
    s"""
UPDATE $tableRef
SET revision = ?, tick = ?
WHERE version = $version
AND $pkNamesEqual
AND revision <= ? AND tick <= ?
    """.trim
  }

  protected def setKeyParms(ps: PreparedStatement, k: K, offset: Int = 0): Int

  protected def createTable(conn: Connection): Unit = createTable(conn, createTableDDL)
  protected def createIndex(conn: Connection): Unit = createIndex(conn, createTickIndexDDL)

  private def getSnapshot(rs: ResultSet): Snapshot = {
    val data = rs.getValue[D](1)(implicitly)
    val revision = rs.getInt(2)
    val tick = rs.getLong(3)
    new Snapshot(data, revision, tick)
  }

  protected def setSnapshot(ps: PreparedStatement, s: Snapshot, offset: Int = 0): Int = {
    ps.setValue(1 + offset, s.content)
    ps.setInt(2 + offset, s.revision)
    ps.setLong(3 + offset, s.tick)
    3 + offset
  }
  protected def setRevTick(ps: PreparedStatement, rev: Int, tick: Long, offset: Int): Unit = {
    ps.setInt(1 + offset, rev)
    ps.setLong(2 + offset, tick)
  }

  protected def readForUpdate[R](key: K)(thunk: (Connection, Option[Snapshot]) => R): R = {
    forUpdate { conn =>
      val existing = getAll(conn, List(key)).get(key)
      thunk(conn, existing)
    }
  }

  def read(key: K): Future[Option[Snapshot]] = readBatch(List(key)).map(_.get(key))
  def readBatch(keys: Iterable[K]): Future[Map[K, Snapshot]] = futureQuery { conn =>
    getAll(conn, keys)
  }

  private def getAll(conn: Connection, keys: Iterable[K], map: Map[K, Snapshot] = Map.empty): Map[K, Snapshot] = {
    val ps = conn.prepareStatement(selectOneSQL)
    try {
      keys.foldLeft(map) {
        case (map, key) =>
          setKeyParms(ps, key)
          val rs = ps.executeQuery()
          try {
            if (rs.next) {
              map.updated(key, getSnapshot(rs))
            } else map
          } finally Try(rs.close)
      }
    } finally Try(ps.close)
  }
  protected def writeSnapshot(conn: Connection, key: K, snapshot: Snapshot): Unit = {
      def updateSnapshot(): Boolean = {
        val ps = conn.prepareStatement(updateSnapshotDefensiveSQL)
        try {
          val snapshotColOffset = setSnapshot(ps, snapshot)
          val offset = setKeyParms(ps, key, snapshotColOffset)
          setRevTick(ps, snapshot.revision, snapshot.tick, offset)
          ps.executeUpdate() == 1
        } finally Try(ps.close)
      }
    // Assume common case, which is update, but insert if not
    if (!updateSnapshot()) {
      // Update failed; can have 2 reasons: 1) Row doesn't exist yet (thus try insert), 2) Old revision and/or tick
      if (insertSnapshots(conn, Map(key -> snapshot)).nonEmpty) { // Could be race condition. Super unlikely, but try update again
        if (!updateSnapshot()) { // At this point, it's probably revision and/or tick
          getAll(conn, List(key)).get(key) match {
            case Some(existing) =>
              throw Exceptions.writeOlder(key, existing, snapshot)
            case None =>
              sys.error(s"Failed to both insert and update for key $key, for unknown reason")
          }
        }
      }
    }
  }

  /** Update snapshots, return failures. */
  protected def updateSnapshots(conn: Connection, snapshots: Map[K, Snapshot]): Seq[K] = if (snapshots.isEmpty) Nil else {
    val isBatch = snapshots.tail.nonEmpty
    val ps = conn prepareStatement updateSnapshotDefensiveSQL
    try {
      val keys = snapshots.toSeq.map {
        case (key, snapshot) =>
          val snapshotColOffset = setSnapshot(ps, snapshot)
          val offset = setKeyParms(ps, key, snapshotColOffset)
          setRevTick(ps, snapshot.revision, snapshot.tick, offset)
          if (isBatch) ps.addBatch()
          key
      }
      if (isBatch) executeBatch(ps, keys)
      else if (ps.executeUpdate() == 1) Nil
      else keys
    } finally Try(ps.close)
  }

  /** Insert snapshots, return failed ids. */
  protected def insertSnapshots(conn: Connection, snapshots: Map[K, Snapshot]): Iterable[K] = if (snapshots.isEmpty) Nil else {
    val isBatch = snapshots.tail.nonEmpty
    val ps = conn.prepareStatement(insertSnapshotSQL)
    try {
      val keys = snapshots.toSeq.map {
        case (key, snapshot) =>
          setParmsOnInsert(ps, key, snapshot)
          if (isBatch) ps.addBatch()
          key
      }
      if (isBatch) executeBatch(ps, keys)
      else if (ps.executeUpdate() == 1) Nil
      else keys
    } catch {
      case e: SQLException if isDuplicateKeyViolation(e) => snapshots.keys
    } finally Try(ps.close)
  }

  def write(key: K, data: Snapshot): Future[Unit] = futureUpdate { conn =>
    writeSnapshot(conn, key, data)
  }
  def writeBatch(snapshots: Map[K, Snapshot]): Future[Unit] =
    if (snapshots.isEmpty) Future successful Unit
    else futureUpdate { conn =>
      val batchInsertFailures = insertSnapshots(conn, snapshots).toSet
      if (batchInsertFailures.nonEmpty) {
        val snapshotsToUpdate = snapshots.filterKeys(batchInsertFailures)
        val batchUpdateFailures = updateSnapshots(conn, snapshotsToUpdate).toSet
        val writeIndividually = snapshotsToUpdate.filterKeys(batchUpdateFailures)
        writeIndividually.foreach {
          case (key, snapshot) => writeSnapshot(conn, key, snapshot)
        }
      }
    }

  protected def refreshKey(conn: Connection)(key: K, revision: Int, tick: Long): Unit =
    refreshAll(conn, Map(key -> (revision -> tick)))

  private def refreshAll(conn: Connection, revisions: Map[K, (Int, Long)]): Unit = {
    val isBatch = revisions.tail.nonEmpty
    val ps = conn.prepareStatement(refreshRevTickDefensiveSQL)
    try {
      revisions.foreach {
        case (key, (revision, tick)) =>
          setRevTick(ps, revision, tick, offset = 0)
          val offset = setKeyParms(ps, key, offset = 2)
          setRevTick(ps, revision, tick, offset = offset)
          if (isBatch) ps.addBatch()
      }
      if (isBatch) executeBatch(ps, Nil)
      else ps.executeUpdate()
    } finally Try(ps.close)
  }

  def refresh(key: K, revision: Int, tick: Long): Future[Unit] =
    refreshBatch(Map(key -> ((revision, tick))))
  def refreshBatch(revisions: Map[K, (Int, Long)]): Future[Unit] =
    if (revisions.isEmpty) Future successful Unit
    else futureUpdate(refreshAll(_, revisions))

  protected def writeIfAbsent(conn: Connection)(key: K, snapshot: Snapshot): Option[Snapshot] = {
    val failed = insertSnapshots(conn, Map(key -> snapshot))
    if (failed.isEmpty) None
    else Some(getAll(conn, failed)(key))
  }

  /**
    *  Write replacement snapshot, if old snapshot matches.
    *  Otherwise return current snapshot.
    *  @return `None` if write was successful, or `Some` current snapshot
    */
  def writeReplacement(conn: Connection)(key: K, oldSnapshot: Snapshot, newSnapshot: Snapshot): Option[Snapshot] = {
    val ps = conn prepareStatement replaceSnapshotSQL
    try {
      val snapshotColOffset = setSnapshot(ps, newSnapshot)
      val offset = setKeyParms(ps, key, snapshotColOffset)
      setRevTick(ps, oldSnapshot.revision, oldSnapshot.tick, offset)
      if (ps.executeUpdate() == 1) None
      else Some(getAll(conn, List(key))(key))
    } finally Try(ps.close)
  }

}

class JdbcStreamProcessStore[PK: ColumnType, D: ColumnType](
    jdbcExeCtx: ExecutionContext, version: Short,
    table: String, schema: Option[String] = None)
  extends AbstractJdbcStreamProcessStore[PK, D](version, table, schema)(implicitly, jdbcExeCtx) {
  cp: ConnectionProvider =>

  def this(jdbcExeCtx: ExecutionContext, version: Short, table: String, schema: String) =
    this(jdbcExeCtx, version, table, schema.optional)

  protected def pkName = "id"

  protected def pkColumns = List(ColumnDef(pkName, implicitly[ColumnType[PK]].typeName))

  protected def setKeyParms(ps: PreparedStatement, key: PK, offset: Int): Int = {
    ps.setValue(1 + offset, key)
    offset + 1
  }

}
