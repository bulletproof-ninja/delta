package delta.jdbc

import java.sql._

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

import delta.process._

import scuff.jdbc.ConnectionSource
import delta.SnapshotReader

import scuff._

object JdbcStreamProcessStore {
  private[jdbc] case class PkColumn[T](name: String, colType: ColumnType[T])

  /** Index definition of index columns. */
  case class Index[S](name: String, first: IndexColumn[S], more: IndexColumn[S]*) {
    def columns = first :: more.toList
  }
  object Index {
    def apply[S](first: IndexColumn[S], more: IndexColumn[S]*) =
      new Index("", first, more: _*)
  }

  /**
   *  Optional column for custom querying and lookups,
   *  extracted from snapshot.
   */
  sealed abstract class IndexColumn[S] {
    def name: String
    def nullable: Boolean
    def colType: ColumnType[Any]
  }
  case class NotNull[S, C: ColumnType](name: String)(val getColumnValue: S => C) extends IndexColumn[S] {
    def nullable = false
    def colType = implicitly[ColumnType[C]].asInstanceOf[ColumnType[Any]]
  }
  case class Nullable[S, C: ColumnType](name: String)(val getColumnValue: S => Option[C]) extends IndexColumn[S] {
    def nullable = true
    def colType = implicitly[ColumnType[C]].asInstanceOf[ColumnType[Any]]
  }

  case class Config(
    pkColumn: String,
    table: String,
    schema: Option[String] = None,
    timestamp: Option[WithTimestamp] = None,
    version: Option[Short] = None) {

    def version(v: Short): Config = copy(version = Some(v))
    def timestamp(wt: WithTimestamp): Config = copy(timestamp = Option(wt))
    def schema(s: String): Config = copy(schema = s.optional)

  }

}

import JdbcStreamProcessStore._

/**
 * Store for keeping track of stream processing, with
 * associated state.
 * NOTE: Pure processes (no processing side effects) should
 * generally use versioning for easier regeneration, BUT
 * side-effecting processing should generally not use versioning,
 * unless the side effect is idempotent, as a new version will
 * trigger re-processing thus the side-effects being repeated.
 */
class JdbcStreamProcessStore[PK: ColumnType, S: ColumnType, U](
  config: Config,
  protected val cs: ConnectionSource,
  blockingCtx: ExecutionContext,
  indexes: List[Index[S]] = Nil)(
  implicit
  protected val updateCodec: UpdateCodec[S, U])
extends AbstractStore(config.version, config.table, config.schema)(blockingCtx)
with StreamProcessStore[PK, S, U] with BlockingCASWrites[PK, S, U, Connection] {

  protected final def dataColumnType = implicitly[ColumnType[S]]

  import config.{ pkColumn => pkColumnName }

  private[this] val setTS: String =
    config.timestamp.map(col => s", ${col.colName} = ${col.sqlFunction}") getOrElse ""

  final protected val WHERE: String = version.map(version => s"WHERE version = $version\nAND") getOrElse "WHERE"
  final protected val pkColumn = PkColumn(pkColumnName, implicitly[ColumnType[PK]])
  final protected val indexColumns: Map[String, IndexColumn[S]] = indexes.flatMap(_.columns).map(c => c.name.toLowerCase -> c).toMap
  protected def versionColType: ColumnType[Short] = ShortColumn
  protected def versionColumn: Option[PkColumn[_]] = version.map(_ => PkColumn("version", versionColType))
  protected def pkColumnDefsDDL(pkColumns: List[PkColumn[_]]): String = {
    pkColumns.map {
      case PkColumn(name, colType) => s"$name ${colType.typeName} NOT NULL"
    }.mkString(",\n  ")
  }

  /**
    * Create a snapshot reader with modified data selection.
    * This can be used to select a subset of the data, to
    * minimize unnecessary data transport.
    * E.g. the regular `SELECT data` can be modified
    * into `SELECT SUBSTR(data, 5) AS data` by providing
    * a `selectData` value of `"SUBSTR(data, 5)"`.
    * @param selectData The modified data selection
    * @return Snapshot with modified data selection
    */
  protected def withModifiedData[ID, MS: ColumnType](
      selectData: String)(
      implicit
      idConv: ID => PK): SnapshotReader[ID, MS] =
    new SnapshotReader[ID, MS] {

      def read(id: ID): Future[Option[Snapshot]] =
        queryModified[MS](id, selectData)

    }

  /**
    * Create a snapshot reader with modified data selection,
    * based on a sub-selection key.
    * This can be used to select a keyed subset of the data,
    * to minimize unnecessary data transport.
    * E.g. if `data` contains a JSON array, the regular
    * `SELECT data` can be modified into
    * `SELECT data -> '$[7]' AS data` by returning
    * a `selectData(7)` value of `"data -> '$[7]'"`
    * (MySQL JSON syntax for selecting the 8th element).
    * @param selectData The modified data selection function
    * @return Snapshot with modified data selection
    */
  protected def withModifiedData[ID, SID, MS: ColumnType](
      selectData: SID => String)(
      implicit
      idConv: ID => PK)
      : SnapshotReader[(ID, SID), MS] =
    new SnapshotReader[(ID, SID), MS] {

      def read(id: (ID, SID)): Future[Option[delta.Snapshot[MS]]] =
        queryModified[MS](id._1, selectData(id._2))

    }

  private def queryModified[MS: ColumnType](
      pk: PK, selectData: String)
      : Future[Option[delta.Snapshot[MS]]] =
    futureQuery { conn =>
      val select = selectOneSQL(selectData)
      queryAll[MS](conn prepareStatement select, pk :: Nil) get pk
    }

  protected def createTableDDL = {
    val pkColumns = versionColumn.toList :+ pkColumn
    val pkColumnDefs = pkColumnDefsDDL(pkColumns)

    val pkNames = pkColumns.map(_.name).mkString(",")
    val qryColumnDefs = if (indexColumns.isEmpty) "" else {
      indexColumns.values.map { col =>
        val nullable = if (col.nullable) "" else " NOT NULL"
        s"""${col.name} ${col.colType.typeName}$nullable"""
      }.mkString("\n  ", ",\n  ", ",")
    }

    val timestampColDef =
      config.timestamp
        .map(col => s"""${col.colName} ${col.sqlType} NOT NULL,""") getOrElse ""

    s"""
CREATE TABLE IF NOT EXISTS $tableRef (
  $pkColumnDefs,$qryColumnDefs
  data ${implicitly[ColumnType[S]].typeName},
  revision INT NOT NULL,
  tick BIGINT NOT NULL,
  $timestampColDef
  PRIMARY KEY ($pkNames)
)"""
  }

  protected def selectOneSQL(dataSelection: String = ""): String =
    if (dataSelection.length == 0) defaultSelectOne
    else defaultSelectOne.replace(
      "SELECT data,",
      s"SELECT $dataSelection AS data,")

  private[this] val defaultSelectOne =
s"""
SELECT data, revision, tick
FROM $tableRef
$WHERE $pkColumnName = ?
"""

  protected def insertSnapshotSQL = _insertSnapshotSQL
  private[this] val _insertSnapshotSQL = {
    val (qryNames, qryQs) = if (indexColumns.isEmpty) "" -> "" else {
      indexColumns.values.map(_.name).mkString("", ", ", ", ") ->
        indexColumns.map(_ => "?").mkString("", ",", ",")
    }
    val (vCol, vQ) = version.map(version => "version, " -> s"$version, ") getOrElse "" -> ""
    val (tCol, tQ) =
      config.timestamp
        .map(col => s"${col.colName}, " -> s"${col.sqlFunction}, ") getOrElse "" -> ""

    s"""
INSERT INTO $tableRef
(${tCol}${vCol}data, revision, tick, $qryNames$pkColumnName)
VALUES ($tQ$vQ?,?,?,$qryQs?)
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
    val qryColumns = (if (indexColumns.isEmpty) "" else ", ") concat
      indexColumns.values.map(col => s"${col.name} = ?").mkString(", ")
    val matches = if (exact) "=" else "<="
    s"""
UPDATE $tableRef
SET data = ?, revision = ?, tick = ?$setTS$qryColumns
$WHERE $pkColumnName = ?
AND revision $matches ? AND tick $matches ?
"""
  }

  protected def refreshRevTickDefensiveSQL = _refreshRevTickDefensiveSQL
  private[this] val _refreshRevTickDefensiveSQL =
    s"""
UPDATE $tableRef
SET revision = ?, tick = ?$setTS
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
    indexes.foreach { index =>
      createCustomIndex(conn)(index)
    }
  }

  protected def getSnapshot[T: ColumnType](rs: ResultSet): delta.Snapshot[T] = {
    val data = rs.getValue[T](1)//(implicitly)
    val revision = rs.getInt(2)
    val tick = rs.getLong(3)
    new delta.Snapshot(data, revision, tick)
  }

  protected def setSnapshot(ps: PreparedStatement, s: Snapshot, offset: Int = 0): Int = {
    ps.setValue(1 + offset, s.content)
    ps.setInt(2 + offset, s.revision)
    ps.setLong(3 + offset, s.tick)
    indexColumns.values.foldLeft(3 + offset) {
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
      val existing = getOne(conn, key)
      thunk(conn, existing)
    }
  }

  def read(key: PK): Future[Option[Snapshot]] =
    futureQuery {
      getOne(_, key)
    }
  def readBatch(keys: Iterable[PK]): Future[Map[PK, Snapshot]] =
    futureQuery {
      getAll(_, keys)
    }

  private def getOne(conn: Connection, key: PK): Option[Snapshot] = {
    queryAll[S](conn prepareStatement selectOneSQL(), key :: Nil) get key
  }

  private def getAll(
      conn: Connection, keys: Iterable[PK])
      : Map[PK, Snapshot] = {
    queryAll[S](conn prepareStatement selectOneSQL(), keys)
  }

  private def queryAll[T: ColumnType](
      ps: PreparedStatement, keys: Iterable[PK])
      : Map[PK, delta.Snapshot[T]] = {
    try {
      keys.foldLeft(Map.empty[PK, delta.Snapshot[T]]) {
        case (map, key) =>
          ps.setValue(1, key)
          val rs = ps.executeQuery()
          try {
            if (rs.next) {
              map.updated(key, getSnapshot[T](rs))
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
          getOne(conn, key) match {
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
    val failed = insertSnapshots(conn, Map(key -> snapshot)).headOption
    failed.flatMap(getOne(conn, _))
  }

  /**
   *  Write replacement snapshot, if old snapshot matches.
   *  Otherwise return current snapshot.
   *  @return `None` if write was successful, or `Some` current snapshot
   */
  protected def writeReplacement(
      conn: Connection)(
      key: PK, oldSnapshot: Snapshot, newSnapshot: Snapshot)
      : Option[Snapshot] = {
    val ps = conn prepareStatement replaceSnapshotSQL
    try {
      val offset = setSnapshot(ps, newSnapshot)
      ps.setValue(1 + offset, key)
      setRevTick(ps, oldSnapshot.revision, oldSnapshot.tick, offset + 1)
      if (ps.executeUpdate() == 1) None
      else getOne(conn, key)
    } finally Try(ps.close)
  }

  /**
   * Query for snapshots matching column values.
   * Uses `AND` semantics, so multiple column
   * queries should not use mutually exclusive
   * values.
   */
  protected def querySnapshot(
    indexColumnMatch: (String, Any), more: (String, Any)*)
    : Future[Map[PK, Snapshot]] = {
    val queryValues: List[(IndexColumn[S], Any)] = (indexColumnMatch :: more.toList).map {
      case (name, value) => indexColumns(name.toLowerCase).asInstanceOf[IndexColumn[S]] -> value
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
            val snapshot = getSnapshot[S](rs)
            map = map.updated(pkColumn.colType.readFrom(rs, 4), snapshot)
          }
          map
        } finally Try(rs.close)
      } finally Try(ps.close)
    }
  }

  /** Lighter version of `querySnapshot` if only existence needed. */
  protected def queryTick(
    indexColumnMatch: (String, Any), more: (String, Any)*)
    : Future[Map[PK, Long]] = {
    val queryValues: List[(IndexColumn[S], Any)] = (indexColumnMatch :: more.toList).map {
      case (name, value) => indexColumns(name.toLowerCase).asInstanceOf[IndexColumn[S]] -> value
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
