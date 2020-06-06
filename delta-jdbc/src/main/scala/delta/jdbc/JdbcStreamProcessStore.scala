package delta.jdbc

import java.sql._

import scala.collection.compat._
import scala.concurrent.Future
import scala.util.Try

import delta.SnapshotReader
import delta.process._

import scuff.jdbc.AsyncConnectionSource

import delta.validation.IndexedStore

object JdbcStreamProcessStore {
  private[jdbc] final case class PkColumn[T](name: String, colType: ColumnType[T])

  /** Index definition of index columns. */
  final case class Index[S](name: String, first: IndexColumn[S], more: IndexColumn[S]*) {
    def columns: List[IndexColumn[S]] = first :: more.toList
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
  final case class NotNull[S, C: ColumnType](name: String)(val getColumnValue: S => C) extends IndexColumn[S] {
    def nullable = false
    def colType = implicitly[ColumnType[C]].asInstanceOf[ColumnType[Any]]
  }
  final case class Nullable[S, C: ColumnType](name: String)(val getColumnValue: S => Option[C]) extends IndexColumn[S] {
    def nullable = true
    def colType = implicitly[ColumnType[C]].asInstanceOf[ColumnType[Any]]
  }

}

import JdbcStreamProcessStore._

/**
 * Store for keeping track of stream processing, with
 * associated state.
 * @note Pure processes (no processing side effects) should
 * generally use versioning for easier regeneration, BUT
 * side-effecting processing should generally not use versioning,
 * unless the side effect is idempotent, as a new version will
 * trigger re-processing thus the side-effects will be repeated.
 */
class JdbcStreamProcessStore[PK: ColumnType, S: ColumnType, U](
  config: Config,
  protected val cs: AsyncConnectionSource,
  indexes: List[Index[S]] = Nil)(
  implicit
  protected val updateCodec: UpdateCodec[S, U])
extends AbstractStore(config.version, config.table, config.schema)
with StreamProcessStore[PK, S, U]
with BlockingCASWrites[PK, S, U, Connection]
with SecondaryIndex
with IndexedStore {

  protected final def dataColumnType = implicitly[ColumnType[S]]

  protected def pkColumnName = config.pkColumn

  private[this] val setTS: String =
    config.timestamp.map(col => s", ${col.columnName} = ${col.sqlFunction}") getOrElse ""

  final protected val WHERE: String = version.map(version => s"WHERE version = $version\nAND") getOrElse "WHERE"
  final protected val pkColumn = PkColumn(pkColumnName, implicitly[ColumnType[PK]])
  final protected val indexColumns: Map[String, IndexColumn[S]] = {
      def different(a: IndexColumn[_], b: IndexColumn[_]): Boolean = {
        a.nullable != b.nullable || a.colType != b.colType
      }
    val columns = indexes.flatMap(_.columns)
    val columnsByName = columns.groupBy(_.name.toLowerCase)
    columnsByName.map {
      case (colName, cols) =>
      val col = cols.head
      cols.tail.find(different(_, col)).foreach { _ =>
        throw new IllegalArgumentException(
          s"Index column $colName is referenced twice, but with different definitions!")
      }
      colName -> col
    }
  }
  final protected val versionColumn: Option[PkColumn[_]] = version.map(_ => PkColumn("version", versionColType))
  protected def versionColType: ColumnType[Short] = ShortColumn
  protected def pkColumnDefsDDL(pkColumns: List[PkColumn[_]]): String = {
    pkColumns.map {
      case PkColumn(name, colType) => s"$name ${colType.typeName} NOT NULL"
    }.mkString(",\n  ")
  }

  /**
    * Create a snapshot reader with modified data selection.
    * This can be used to select a subset of the data, to
    * minimize unnecessary data transport.
    * E.g. the regular `data` column can be modified
    * into `SUBSTR(data, 5)` by providing
    * a `selectData` value of `"SUBSTR(data, 5)"`.
    * @tparam M Modified data type
    * @param selectData The modified data selection
    * @return Snapshot with modified data selection
    */
  protected def withModifiedData[M: ReadColumn](
      selectData: String): SnapshotReader[PK, M] =
    new SnapshotReader[PK, M] {
      def read(id: PK): Future[Option[Snapshot]] =
        queryModified[M](id, selectData)
    }

  /**
    * Create a snapshot reader with modified data selection,
    * based on a sub-selection key.
    * This can be used to select a keyed subset of the data,
    * to minimize unnecessary data transport.
    * E.g. if `data` contains a JSON array, the regular
    * `data` column can be modified into
    * `data -> '$[7]' AS data` by returning
    * a `selectData(7)` value of `"data -> '$[7]'"`
    * (MySQL JSON syntax for selecting the 8th element).
    * @tparam SK Secondary key type
    * @tparam M Modified data type
    * @param selectData The modified data selection function
    * @return Snapshot with modified data selection
    */
  protected def withModifiedData[SK, M: ReadColumn](
      selectData: SK => String)
      : SnapshotReader[(PK, SK), M] =
    new SnapshotReader[(PK, SK), M] {

      def read(id: (PK, SK)): Future[Option[Snapshot]] =
        queryModified[M](id._1, selectData(id._2))

    }

  private def queryModified[MS: ReadColumn](
      pk: PK, selectData: String)
      : Future[Option[delta.Snapshot[MS]]] =
    futureQuery { conn =>
      val select = selectOneSQL(selectData)
      conn.prepare(select) {
        queryAll[MS](_, pk :: Nil) get pk
      }
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
        .map(col => s"""${col.columnName} ${col.sqlType} NOT NULL,""") getOrElse ""

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
        .map(col => s"${col.columnName}, " -> s"${col.sqlFunction}, ") getOrElse "" -> ""

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
    val columnNames = {
      versionColumn.map(_.name).toList ++ index.columns.map(_.name)
    }.mkString(", ")
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

  protected def getSnapshot[T: ReadColumn](rs: ResultSet): delta.Snapshot[T] = {
    val data = rs.getValue[T](1)
    val revision = rs.getInt(2)
    val tick = rs.getLong(3)
    new delta.Snapshot(data, revision, tick)
  }

  protected def setSnapshot(ps: PreparedStatement, s: Snapshot, offset: Int = 0): Int = {
    ps.setValue(1 + offset, s.state)
    ps.setInt(2 + offset, s.revision)
    ps.setLong(3 + offset, s.tick)
    indexColumns.values.foldLeft(3 + offset) {
      case (offset, qryCol) =>
        val value = qryCol match {
          case qryCol @ NotNull(_) => qryCol.getColumnValue(s.state)
          case qryCol @ Nullable(_) => qryCol.getColumnValue(s.state).orNull
        }
        ps.setValue(1 + offset, value)(qryCol.colType)
        offset + 1
    }
  }
  protected def setRevTick(ps: PreparedStatement, rev: Revision, tick: Tick, offset: Int): Unit = {
    ps.setInt(1 + offset, rev)
    ps.setLong(2 + offset, tick)
  }

  protected def readForUpdate[R](key: PK)(thunk: (Connection, Option[Snapshot]) => R): Future[R] = {
    cs.asyncUpdate { conn =>
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

  private def getOne(conn: Connection, key: PK): Option[Snapshot] =
    conn.prepare(selectOneSQL()) {
      queryAll[S](_, key :: Nil) get key
    }

  private def getAll(
      conn: Connection, keys: Iterable[PK])
      : Map[PK, Snapshot] =
    conn.prepare(selectOneSQL()) {
      queryAll[S](_, keys)
    }

  private def queryAll[T: ReadColumn](
      ps: PreparedStatement, keys: Iterable[PK])
      : Map[PK, delta.Snapshot[T]] =
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

  protected def writeSnapshot(conn: Connection, key: PK, snapshot: Snapshot): Unit = {
      def updateSnapshot(): Boolean =
        conn.prepare(updateSnapshotDefensiveSQL) { ps =>
          val offset = setSnapshot(ps, snapshot)
          ps.setValue(1 + offset, key)
          setRevTick(ps, snapshot.revision, snapshot.tick, 1 + offset)
          ps.executeUpdate() == 1
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
    if (snapshots.isEmpty) Nil
    else conn.prepare(updateSnapshotDefensiveSQL) { ps =>
      val isBatch = snapshots.size > 1
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
    }

  /** Insert snapshots, return failed ids. */
  protected def insertSnapshots(conn: Connection, snapshots: collection.Map[PK, Snapshot]): Iterable[PK] =
    if (snapshots.isEmpty) Nil
    else conn.prepare(insertSnapshotSQL) { ps =>
      val isBatch = snapshots.size > 1
      val keys = snapshots.map {
        case (key, snapshot) =>
          setParmsOnInsert(ps, key, snapshot)
          if (isBatch) ps.addBatch()
          key
      }
      try {
        if (isBatch) executeBatch(ps, keys)
        else if (ps.executeUpdate() == 1) Nil
        else keys
      } catch {
        case e: SQLException if isDuplicateKeyViolation(e) => snapshots.keys
      }
    }

  def write(key: PK, data: Snapshot): Future[Unit] = futureUpdate { conn =>
    writeSnapshot(conn, key, data)
  }
  def writeBatch(snapshots: collection.Map[PK, Snapshot]): Future[Unit] =
    if (snapshots.isEmpty) Future.unit
    else futureUpdate { conn =>
      val batchInsertFailures = insertSnapshots(conn, snapshots).toSet
      if (batchInsertFailures.nonEmpty) {
        val snapshotsToUpdate = snapshots.view.filterKeys(batchInsertFailures).toMap
        val batchUpdateFailures = updateSnapshots(conn, snapshotsToUpdate).toSet
        val writeIndividually = snapshotsToUpdate.view.filterKeys(batchUpdateFailures)
        writeIndividually.foreach {
          case (key, snapshot) => writeSnapshot(conn, key, snapshot)
        }
      }
    }

  protected def refreshKey(conn: Connection)(key: PK, revision: Revision, tick: Tick): Unit =
    refreshAll(conn, Map(key -> (revision -> tick)))

  private def refreshAll(conn: Connection, revisions: collection.Map[PK, (Int, Long)]): Unit =
    conn.prepare(refreshRevTickDefensiveSQL) { ps =>
      val isBatch = revisions.size > 1
      revisions.foreach {
        case (key, (revision, tick)) =>
          setRevTick(ps, revision, tick, offset = 0)
          ps.setValue(3, key)
          setRevTick(ps, revision, tick, offset = 3)
          if (isBatch) ps.addBatch()
      }
      if (isBatch) executeBatch(ps, Nil)
      else ps.executeUpdate()
    }

  def refresh(key: PK, revision: Revision, tick: Tick): Future[Unit] =
    refreshBatch(Map(key -> ((revision, tick))))
  def refreshBatch(revisions: collection.Map[PK, (Int, Long)]): Future[Unit] =
    if (revisions.isEmpty) Future.unit
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
      : Option[Snapshot] =
    conn.prepare(replaceSnapshotSQL) { ps =>
      val offset = setSnapshot(ps, newSnapshot)
      ps.setValue(1 + offset, key)
      setRevTick(ps, oldSnapshot.revision, oldSnapshot.tick, offset + 1)
      if (ps.executeUpdate() == 1) None
      else getOne(conn, key)
    }

  protected type QueryValue = Any

  protected def querySnapshot(
      indexColumnMatch: (String, QueryValue), more: (String, QueryValue)*)
      : Future[Map[PK, Snapshot]] = {

    val queryValues: List[(IndexColumn[S], Any)] = (indexColumnMatch :: more.toList).map {
      case (name, value) => indexColumns(name.toLowerCase).asInstanceOf[IndexColumn[S]] -> value
    }

    val where = queryValues.map {
      case (col, _) => s"${col.name} = ?"
    }.mkString(" AND ")

    val select = s"""
SELECT data, revision, tick, $pkColumnName
FROM $tableRef
$WHERE $where
"""

    cs.asyncQuery { conn =>
      conn.prepare(select) { ps =>
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
      }
    }

  }

  protected def queryTick(
      indexColumnMatch: (String, QueryValue), more: (String, QueryValue)*)
      : Future[Map[PK, Tick]] = {

    val queryValues: List[(IndexColumn[S], Any)] = (indexColumnMatch :: more.toList).map {
      case (name, value) => indexColumns(name.toLowerCase).asInstanceOf[IndexColumn[S]] -> value
    }

    val where = queryValues.map {
      case (col, _) => s"${col.name} = ?"
    }.mkString(" AND ")

    val select = s"""
SELECT tick, $pkColumnName
FROM $tableRef
$WHERE $where
"""

    cs.asyncQuery { conn =>
      conn.prepare(select) { ps =>
        queryValues.zipWithIndex.foreach {
          case ((col, value), idx) =>
            ps.setValue(idx + 1, value)(col.colType)
        }
        val rs = ps.executeQuery()
        try {
          var map = Map.empty[PK, Tick]
          while (rs.next) {
            val tick = rs.getLong(1)
            val pk = rs.getValue[PK](2)
            map = map.updated(pk, tick)
          }
          map
        } finally Try(rs.close)
      }
    }

  }

  private[this] val WHERE_for_d = version.map(v => s"\n  WHERE d.version=$v\n") getOrElse ""
  private[this] val WHERE_for_o = WHERE.replace(" version", " o.version")

  protected def selectDuplicatesSQL(indexColumn: String): String = s"""
SELECT o.$indexColumn, o.$pkColumnName, o.tick
FROM $tableRef AS o
$WHERE_for_o o.$indexColumn IN (
  SELECT d.$indexColumn
  FROM $tableRef AS d$WHERE_for_d
  GROUP BY d.$indexColumn
  HAVING COUNT(*) > 1
)
"""

  protected type Ref[V] = ReadColumn[V]

  /**
    * Find duplicates in index.
    * @param colName the column to search for duplicates, of type `V`
    * @return `Map` of duplicates
    */
  protected def findDuplicates[V](
      colName: String)(
      implicit col: ReadColumn[V]): Future[Map[V, Map[PK, Tick]]] =
    cs.asyncQuery { conn =>
      val sql = selectDuplicatesSQL(colName)
      conn.prepare(sql) { ps =>
        val rs = ps.executeQuery()
        try {
          var outer: Map[V, Map[PK, Tick]] = Map.empty
          while(rs.next) {
            val dupe = rs.getValue[V](1)
            val pk = rs.getValue[PK](2)
            val tick = rs.getLong(3)
            val inner = outer.getOrElse(dupe, Map.empty)
            outer = outer.updated(dupe, inner.updated(pk, tick))
          }
          outer
        } finally Try(rs.close)
      }

    }


}
