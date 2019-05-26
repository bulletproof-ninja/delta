package delta.jdbc

import java.sql._

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

import delta.process.{ StreamProcessStore, BlockingCASWrites, Exceptions }

import scuff.concurrent.Threads
import scuff.jdbc.ConnectionSource

object JdbcStreamProcessStore {
  private case class PkColumn[T](name: String, colType: ColumnType[T])

  /** Index definition of query columns. */
  case class Index[S](name: String, first: QueryColumn[S], more: QueryColumn[S]*) {
    def columns = first :: more.toList
  }
  object Index {
    def apply[S](first: QueryColumn[S], more: QueryColumn[S]*) =
      new Index("", first, more: _*)
  }

  /**
   *  Optional column for custom querying and lookups,
   *  extracted from snapshot.
   */
  sealed abstract class QueryColumn[S] {
    def name: String
    def nullable: Boolean
    def colType: ColumnType[Any]
  }
  case class NotNull[S, C: ColumnType](name: String)(val getColumnValue: S => C) extends QueryColumn[S] {
    def nullable = false
    def colType = implicitly[ColumnType[C]].asInstanceOf[ColumnType[Any]]
  }
  case class Nullable[S, C: ColumnType](name: String)(val getColumnValue: S => Option[C]) extends QueryColumn[S] {
    def nullable = true
    def colType = implicitly[ColumnType[C]].asInstanceOf[ColumnType[Any]]
  }
}

import JdbcStreamProcessStore._

/**
 * Store for keeping track of stream processing, with
 * associated state.
 * NOTE: Pure processes (no processing side effects) should
 * generally use versioning for easier regeneration, BUT
 * side-effecting processing should *never* use versioning,
 * as a new version can trigger reprocessing with the
 * side-effects being repeated.
 */
class JdbcStreamProcessStore[PK: ColumnType, S: ColumnType](
    pkColumnName: String, customIndexes: List[Index[S]],
    cs: ConnectionSource,
    version: Option[Short],
    table: String, schema: Option[String],
    blockingCtx: ExecutionContext)
  extends AbstractStore(cs, version, table, schema)(blockingCtx)
  with StreamProcessStore[PK, S] with BlockingCASWrites[PK, S, Connection] {

  def this(
      pkColumnName: String,
      cs: ConnectionSource,
      version: Option[Short],
      table: String, schema: Option[String],
      blockingCtx: ExecutionContext) =
    this(pkColumnName, Nil, cs, version, table, schema, blockingCtx)

  def this(
      cs: ConnectionSource,
      version: Option[Short],
      table: String, schema: Option[String],
      blockingCtx: ExecutionContext) =
    this("stream", Nil, cs, version, table, schema, blockingCtx)

  def this(
      customIndexes: List[Index[S]],
      cs: ConnectionSource,
      version: Option[Short],
      table: String, schema: Option[String],
      blockingCtx: ExecutionContext) =
    this("stream", customIndexes, cs, version, table, schema, blockingCtx)

  protected val WHERE: String = version.map(version => s"WHERE version = $version\nAND") getOrElse "WHERE"

  private[this] val pkColumn = PkColumn(pkColumnName, implicitly[ColumnType[PK]])
  private[this] val queryColumns: Map[String, QueryColumn[S]] = customIndexes.flatMap(_.columns).map(c => c.name -> c).toMap
  protected def versionColType: ColumnType[Short] = ShortColumn

  protected def createTableDDL = {
    val pkColumns = version.map(_ => PkColumn("version", versionColType)).toList :+ pkColumn
    val pkColumnDefs = pkColumns.map {
      case PkColumn(name, colType) => s"$name ${colType.typeName} NOT NULL"
    }.mkString(",\n  ")

    val pkNames = pkColumns.map(_.name).mkString(",")
    val qryColumnDefs = if (queryColumns.isEmpty) "" else {
      queryColumns.values.map { col =>
        val nullable = if (col.nullable) "" else " NOT NULL"
        s"""${col.name} ${col.colType.typeName}$nullable"""
      }.mkString("\n  ", ",\n  ", ",")
    }

    s"""
CREATE TABLE IF NOT EXISTS $tableRef (
  $pkColumnDefs,$qryColumnDefs
  data ${implicitly[ColumnType[S]].typeName},
  revision INT NOT NULL,
  tick BIGINT NOT NULL,

  PRIMARY KEY ($pkNames)
)"""
  }

  protected def selectOneSQL = _selectOneSQL
  private[this] val _selectOneSQL =
    s"""
SELECT data, revision, tick
FROM $tableRef
$WHERE $pkColumnName = ?
"""

  protected def insertSnapshotSQL = _insertSnapshotSQL
  private[this] val _insertSnapshotSQL = {
    val (qryNames, qryQs) = if (queryColumns.isEmpty) "" -> "" else {
      queryColumns.keys.mkString("", ", ", ", ") ->
        queryColumns.map(_ => "?").mkString("", ",", ",")
    }
    val (vCol, vQ) = version.map(version => "version, " -> s"$version, ") getOrElse "" -> ""
    s"""
INSERT INTO $tableRef
(${vCol}data, revision, tick, $qryNames$pkColumnName)
VALUES ($vQ?,?,?,$qryQs?)
"""
  }
  protected def setParmsOnInsert(ps: PreparedStatement, key: PK, snapshot: Snapshot): Int = {
    val offset = setSnapshot(ps, snapshot)
    ps.setValue(1 + offset, key)
    offset + 1
  }

  protected def replaceSnapshotSQL = _replaceSnapshotSQL
  private[this] val _replaceSnapshotSQL = updateSnapshotSQL(exact = true)

  protected def updateSnapshotDefensiveSQL = _updateSnapshotDefensiveSQL
  private[this] val _updateSnapshotDefensiveSQL = updateSnapshotSQL(exact = false)

  protected def updateSnapshotSQL(exact: Boolean): String = {
    val qryColumns = (if (queryColumns.isEmpty) "" else ", ") concat
      queryColumns.keys.map(name => s"$name = ?").mkString(", ")
    val matches = if (exact) "=" else "<="
    s"""
UPDATE $tableRef
SET data = ?, revision = ?, tick = ?$qryColumns
$WHERE $pkColumnName = ?
AND revision $matches ? AND tick $matches ?
"""
  }

  protected def refreshRevTickDefensiveSQL = _refreshRevTickDefensiveSQL
  private[this] val _refreshRevTickDefensiveSQL =
    s"""
UPDATE $tableRef
SET revision = ?, tick = ?
$WHERE $pkColumnName = ?
AND revision <= ? AND tick <= ?
"""

  protected def createCustomIndexDDL(index: JdbcStreamProcessStore.Index[S]): String = {
    val indexName = if (index.name != "") index.name else index.columns.map(_.name).mkString("idx_", "_", "")
    val columnNames = index.columns.map(_.name).mkString(", ")
    s"""
CREATE INDEX IF NOT EXISTS $indexName
  ON $tableRef ($columnNames)
"""
  }

  protected def createTable(conn: Connection): Unit = createTable(conn, createTableDDL)
  protected def createTickIndex(conn: Connection): Unit = createIndex(conn, createTickIndexDDL)
  protected def createCustomIndex(conn: Connection)(index: JdbcStreamProcessStore.Index[S]): Unit =
    createIndex(conn, createCustomIndexDDL(index))

  override protected def ensureTable(conn: Connection): Unit = {
    super.ensureTable(conn)
    customIndexes.foreach { index =>
      createCustomIndex(conn)(index)
    }
  }

  protected def getSnapshot(rs: ResultSet): Snapshot = {
    val data = rs.getValue[S](1)(implicitly)
    val revision = rs.getInt(2)
    val tick = rs.getLong(3)
    new Snapshot(data, revision, tick)
  }

  protected def setSnapshot(ps: PreparedStatement, s: Snapshot, offset: Int = 0): Int = {
    ps.setValue(1 + offset, s.content)
    ps.setInt(2 + offset, s.revision)
    ps.setLong(3 + offset, s.tick)
    queryColumns.values.foldLeft(3 + offset) {
      case (offset, qryCol) =>
        val value = qryCol match {
          case qryCol @ NotNull(_) => qryCol.getColumnValue(s.content)
          case qryCol @ Nullable(_) => qryCol.getColumnValue(s.content).orNull
        }
        ps.setValue(1 + offset, value)(qryCol.colType)
        offset + 1
    }
  }
  protected def setRevTick(ps: PreparedStatement, rev: Int, tick: Long, offset: Int): Unit = {
    ps.setInt(1 + offset, rev)
    ps.setLong(2 + offset, tick)
  }

  protected def readForUpdate[R](key: PK)(thunk: (Connection, Option[Snapshot]) => R): R = {
    cs.forUpdate { conn =>
      val existing = getAll(conn, List(key)).get(key)
      thunk(conn, existing)
    }
  }

  def read(key: PK): Future[Option[Snapshot]] = readBatch(List(key)).map(_.get(key))(Threads.PiggyBack)
  def readBatch(keys: Iterable[PK]): Future[Map[PK, Snapshot]] = futureQuery { conn =>
    getAll(conn, keys)
  }

  private def getAll(conn: Connection, keys: Iterable[PK], map: Map[PK, Snapshot] = Map.empty): Map[PK, Snapshot] = {
    val ps = conn.prepareStatement(selectOneSQL)
    try {
      keys.foldLeft(map) {
        case (map, key) =>
          ps.setValue(1, key)
          val rs = ps.executeQuery()
          try {
            if (rs.next) {
              map.updated(key, getSnapshot(rs))
            } else map
          } finally Try(rs.close)
      }
    } finally Try(ps.close)
  }
  protected def writeSnapshot(conn: Connection, key: PK, snapshot: Snapshot): Unit = {
      def updateSnapshot(): Boolean = {
        val ps = conn.prepareStatement(updateSnapshotDefensiveSQL)
        try {
          val offset = setSnapshot(ps, snapshot)
          ps.setValue(1 + offset, key)
          setRevTick(ps, snapshot.revision, snapshot.tick, 1 + offset)
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
  protected def updateSnapshots(conn: Connection, snapshots: collection.Map[PK, Snapshot]): Iterable[PK] =
    if (snapshots.isEmpty) Nil else {
      val isBatch = snapshots.tail.nonEmpty
      val ps = conn prepareStatement updateSnapshotDefensiveSQL
      try {
        val keys = snapshots.map {
          case (key, snapshot) =>
            val offset = setSnapshot(ps, snapshot)
            ps.setValue(1 + offset, key)
            setRevTick(ps, snapshot.revision, snapshot.tick, 1 + offset)
            if (isBatch) ps.addBatch()
            key
        }
        if (isBatch) executeBatch(ps, keys)
        else if (ps.executeUpdate() == 1) Nil
        else keys
      } finally Try(ps.close)
    }

  /** Insert snapshots, return failed ids. */
  protected def insertSnapshots(conn: Connection, snapshots: collection.Map[PK, Snapshot]): Iterable[PK] =
    if (snapshots.isEmpty) Nil else {
      val isBatch = snapshots.tail.nonEmpty
      val ps = conn.prepareStatement(insertSnapshotSQL)
      try {
        val keys = snapshots.map {
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

  def write(key: PK, data: Snapshot): Future[Unit] = futureUpdate { conn =>
    writeSnapshot(conn, key, data)
  }
  def writeBatch(snapshots: collection.Map[PK, Snapshot]): Future[Unit] =
    if (snapshots.isEmpty) Future successful (())
    else futureUpdate { conn =>
      val batchInsertFailures = insertSnapshots(conn, snapshots).toSet
      if (batchInsertFailures.nonEmpty) {
        val snapshotsToUpdate = snapshots.filterKeys(batchInsertFailures).toMap
        val batchUpdateFailures = updateSnapshots(conn, snapshotsToUpdate).toSet
        val writeIndividually = snapshotsToUpdate.filterKeys(batchUpdateFailures)
        writeIndividually.foreach {
          case (key, snapshot) => writeSnapshot(conn, key, snapshot)
        }
      }
    }

  protected def refreshKey(conn: Connection)(key: PK, revision: Int, tick: Long): Unit =
    refreshAll(conn, Map(key -> (revision -> tick)))

  private def refreshAll(conn: Connection, revisions: collection.Map[PK, (Int, Long)]): Unit = {
    val isBatch = revisions.tail.nonEmpty
    val ps = conn.prepareStatement(refreshRevTickDefensiveSQL)
    try {
      revisions.foreach {
        case (key, (revision, tick)) =>
          setRevTick(ps, revision, tick, offset = 0)
          ps.setValue(3, key)
          setRevTick(ps, revision, tick, offset = 3)
          if (isBatch) ps.addBatch()
      }
      if (isBatch) executeBatch(ps, Nil)
      else ps.executeUpdate()
    } finally Try(ps.close)
  }

  def refresh(key: PK, revision: Int, tick: Long): Future[Unit] =
    refreshBatch(Map(key -> ((revision, tick))))
  def refreshBatch(revisions: collection.Map[PK, (Int, Long)]): Future[Unit] =
    if (revisions.isEmpty) Future successful (())
    else futureUpdate(refreshAll(_, revisions))

  protected def writeIfAbsent(conn: Connection)(key: PK, snapshot: Snapshot): Option[Snapshot] = {
    val failed = insertSnapshots(conn, Map(key -> snapshot))
    if (failed.isEmpty) None
    else Some(getAll(conn, failed)(key))
  }

  /**
   *  Write replacement snapshot, if old snapshot matches.
   *  Otherwise return current snapshot.
   *  @return `None` if write was successful, or `Some` current snapshot
   */
  def writeReplacement(conn: Connection)(key: PK, oldSnapshot: Snapshot, newSnapshot: Snapshot): Option[Snapshot] = {
    val ps = conn prepareStatement replaceSnapshotSQL
    try {
      val offset = setSnapshot(ps, newSnapshot)
      ps.setValue(1 + offset, key)
      setRevTick(ps, oldSnapshot.revision, oldSnapshot.tick, offset + 1)
      if (ps.executeUpdate() == 1) None
      else Some(getAll(conn, List(key))(key))
    } finally Try(ps.close)
  }

  protected def querySnapshot(queryColumnMatch: (String, Any), more: (String, Any)*): Future[Map[PK, Snapshot]] = {
    val queryValues: List[(QueryColumn[S], Any)] = (queryColumnMatch :: more.toList).map {
      case (name, value) => queryColumns(name).asInstanceOf[QueryColumn[S]] -> value
    }
    val where = queryValues.map {
      case (col, _) => s"${col.name} = ?"
    }.mkString(" AND ")
    val select =
      s"""
SELECT data, revision, tick, $pkColumnName
FROM $tableRef
$WHERE $where
"""
    futureQuery { conn =>
      val ps = conn.prepareStatement(select)
      try {
        queryValues.zipWithIndex.foreach {
          case ((col, value), idx) =>
            ps.setValue(idx + 1, value)(col.colType)
        }
        val rs = ps.executeQuery()
        try {
          var map = Map.empty[PK, Snapshot]
          while (rs.next) {
            val snapshot = getSnapshot(rs)
            map = map.updated(pkColumn.colType.readFrom(rs, 4), snapshot)
          }
          map
        } finally Try(rs.close)
      } finally Try(ps.close)
    }
  }

  protected def queryTick(queryColumnMatch: (String, Any), more: (String, Any)*): Future[Map[PK, Long]] = {
    val queryValues: List[(QueryColumn[S], Any)] = (queryColumnMatch :: more.toList).map {
      case (name, value) => queryColumns(name).asInstanceOf[QueryColumn[S]] -> value
    }
    val where = queryValues.map {
      case (col, _) => s"${col.name} = ?"
    }.mkString(" AND ")
    val select =
      s"""
SELECT tick, $pkColumnName
FROM $tableRef
$WHERE $where
"""
    futureQuery { conn =>
      val ps = conn.prepareStatement(select)
      try {
        queryValues.zipWithIndex.foreach {
          case ((col, value), idx) =>
            ps.setValue(idx + 1, value)(col.colType)
        }
        val rs = ps.executeQuery()
        try {
          var map = Map.empty[PK, Long]
          while (rs.next) {
            val tick = rs.getLong(1)
            val pk = rs.getValue[PK](2)
            map = map.updated(pk, tick)
          }
          map
        } finally Try(rs.close)
      } finally Try(ps.close)
    }
  }

}
