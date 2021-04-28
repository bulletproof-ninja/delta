package delta.jdbc

import delta.jdbc.JdbcStreamProcessStore.PkColumn

import scala.concurrent.Future
import scala.collection.compat._

import java.sql.{ Connection, PreparedStatement, ResultSet }
import scuff.Reduction

object IndexTableSupport {
  final case class IndexTable[S, C: ColumnType](
      indexColumn: String)(
      indexValues: PartialFunction[S, Iterable[C]]) {
    def getIndexValues(state: S): Set[Any] =
      if (indexValues isDefinedAt state) indexValues(state).toSet
      else Set.empty
    def colType = implicitly[ColumnType[C]].asInstanceOf[ColumnType[Any]]
  }
}

/**
 * Enable indexed lookup for many-to-one associations,
 * by creating one or more index tables.
 * @note For simple one-to-one associations,
 * use [[delta.jdbc.JdbcStreamProcessStore.Index]]
 */
trait IndexTableSupport[PK, S, U]
extends JdbcStreamProcessStore[PK, S, U] {

  protected type IndexTable = IndexTableSupport.IndexTable[S, _]
  protected def IndexTable[C: ColumnType](
      indexColumn: String)(
      indexValues: PartialFunction[S, Iterable[C]]): IndexTable =
    IndexTableSupport.IndexTable[S, C](indexColumn)(indexValues)

  protected def indexTables: List[IndexTable]
  private lazy val indexTablesByColumn = {
    val map = indexTables.map(t => t.indexColumn.toLowerCase -> t).toMap[String, IndexTable]
    require(map.size == indexTables.size, s"Name clash on index tables (perhaps a casing issue?)")
    map
  }

  protected def indexTableRef(table: IndexTable): String = s"${tableRef}_${table.indexColumn}"

  protected def createIndexTableDDL(table: IndexTable): String = {
    val pkColumns = versionColumn.toList :+ PkColumn(table.indexColumn, table.colType) :+ pkColumn
    val pkColumnDefs = pkColumnDefsDDL(pkColumns)
    val pkNames = pkColumns.map(_.name).mkString(", ")
    val fkNames = (versionColumn.map(_.name).toList :+ pkColumn.name).mkString(", ")
    s"""
CREATE TABLE IF NOT EXISTS ${indexTableRef(table)} (
  $pkColumnDefs,
  PRIMARY KEY ($pkNames),
  FOREIGN KEY ($fkNames)
    REFERENCES $tableRef($fkNames)
)"""
  }

  private final val DELETE_ALL = -1

  protected def deleteRowsSQL(table: IndexTable, deleteCount: Int = 1): String = {
    val AND = deleteCount match {
      case DELETE_ALL => ""
      case 0 => sys.error("`deleteCount = 0` is nonsensical")
      case 1 => s"\nAND ${table.indexColumn} = ?"
      case n => Iterator.fill(n)("?").mkString(s"\nAND ${table.indexColumn} IN (", ",", ")")
    }
    s"""
DELETE FROM ${indexTableRef(table)}
$WHERE ${pkColumn.name} = ?$AND
"""
  }
  protected def insertRowSQL(table: IndexTable): String = {
    val (vCol, vQ) = version.map(version => "version, " -> s"$version, ") getOrElse "" -> ""
    s"""
INSERT INTO ${indexTableRef(table)}
($vCol${table.indexColumn}, ${pkColumn.name})
VALUES ($vQ?,?)
"""
  }

  protected def deleteAsBatchThreshold: Int = 10

  private def deleteRowsAsBatch(ps: PreparedStatement, stream: PK, table: IndexTable, keys: Set[Any]): Unit = {
    keys.foreach { key =>
      ps.setValue(1, stream)(pkColumn.colType)
      ps.setValue(2, key)(table.colType)
      ps.addBatch()
    }
    ps.executeBatch()
  }

  private def deleteRowsAsOne(ps: PreparedStatement, stream: PK, table: IndexTable, keys: Set[Any]): Unit = {
    ps.setValue(1, stream)(pkColumn.colType)
    keys.iterator.zipWithIndex.foreach {
      case (key, idx) => ps.setValue(2 + idx, key)(table.colType)
    }
    ps.executeUpdate()
  }

  private[this] val ALL_KEYS = Set.empty[Any]

  protected def deleteIndexTableRows(
      conn: Connection, stream: PK)(
      table: IndexTable, keys: Set[Any]): Unit = {
    val deleteAsBatch = keys.size >= deleteAsBatchThreshold
    val sql =
      if (keys eq ALL_KEYS) deleteRowsSQL(table, DELETE_ALL)
      else if (deleteAsBatch) deleteRowsSQL(table)
      else deleteRowsSQL(table, keys.size)
    conn.prepare(sql) { ps =>
      if (deleteAsBatch) deleteRowsAsBatch(ps, stream, table, keys)
      else deleteRowsAsOne(ps, stream, table, keys)
    }
  }

  protected def insertIndexTableRows(conn: Connection, stream: PK)(table: IndexTable, keys: Set[Any]): Unit = {
    assert(keys.nonEmpty)
    val isBatch = keys.size > 1
    conn.prepare(insertRowSQL(table)) { ps =>
      keys.foreach { key =>
        ps.setValue(1, key)(table.colType)
        ps.setValue(2, stream)(pkColumn.colType)
        if (isBatch) ps.addBatch()
      }
      if (isBatch) ps.executeBatch()
      else ps.executeUpdate()
    }
  }

  override protected def ensureTable(conn: Connection): Unit = {
    super.ensureTable(conn)
    indexTables.foreach { table =>
      val ddl = createIndexTableDDL(table)
      createTable(conn, ddl)
    }
  }

  override protected def updateSnapshots(
      conn: Connection, snapshots: scala.collection.Map[PK,Snapshot])
      : Iterable[PK] = {
    val failed = super.updateSnapshots(conn, snapshots).toSet
    val succeeded = snapshots.toMap -- failed
    indexTables.foreach { table =>
      succeeded.foreach {
        case (key, snapshot) =>
          deleteIndexTableRows(conn, key)(table, ALL_KEYS)
          insertIndexValues(conn, table)(key, snapshot.state)
      }
    }
    failed
  }

  override protected def insertSnapshots(
      conn: Connection, snapshots: scala.collection.Map[PK,Snapshot])
      : Iterable[PK] = {
    val failed = super.insertSnapshots(conn, snapshots).toSet
    val succeeded = snapshots.toMap -- failed
    indexTables.foreach { table =>
      succeeded.foreach {
        case (key, snapshot) =>
          insertIndexValues(conn, table)(key, snapshot.state)
      }
    }
    failed
  }

  override protected def writeIfAbsent(conn: Connection)(
      key: PK, snapshot: Snapshot): Option[Snapshot] =
    super.writeIfAbsent(conn)(key, snapshot) // <- delegates to `insertSnapshots`

  override protected def writeReplacement(conn: Connection)(
      key: PK, oldSnapshot: Snapshot, newSnapshot: Snapshot): Option[Snapshot] = {
    super.writeReplacement(conn)(key, oldSnapshot, newSnapshot) orElse {
      indexTables.foreach { table =>
        updateIndexValues(conn, table)(key, oldSnapshot.state, newSnapshot.state)
      }
      None
    }
  }

  private def insertIndexValues(
      conn: Connection, table: IndexTable)(
      key: PK, state: S): Unit = {
    val keyValues = table getIndexValues state
    if (keyValues.nonEmpty) insertIndexTableRows(conn, key)(table, keyValues)
  }


  private def updateIndexValues(
      conn: Connection, table: IndexTable)(
      key: PK,
      oldState: S, newState: S): Unit = {
    val oldValues = table getIndexValues oldState
    val newValues = table getIndexValues newState
    if (oldValues ne newValues) { // <- cheap, but obviously unreliable check
      val deleteValues = oldValues diff newValues
      if (deleteValues.nonEmpty) deleteIndexTableRows(conn, key)(table, deleteValues)
      val insertValues = newValues diff oldValues
      if (insertValues.nonEmpty) insertIndexTableRows(conn, key)(table, insertValues)
    }
  }

  override protected def bulkSelect[C, R](
      filter: Seq[(String, QueryValue)],
      consumer: Reduction[(StreamId, C), R],
      extractColumns: ResultSet => C,
      selectColumns: String*)
      : Future[R] = {

    val columnMatches = filter.map(e => e._1.toLowerCase -> e._2)
    val useIndexTables: Map[IndexTableSupport.IndexTable[S, _],QueryValue] =
      columnMatches
        .flatMap {
          case (columnName, matchValue) =>
            indexTablesByColumn.get(columnName).map(_ -> matchValue)
        }
        .toMap
    if (useIndexTables.isEmpty) {
      super.bulkSelect(filter, consumer, extractColumns, selectColumns: _*)
    } else {
      val superAlias = 's'
      val superFilter = (columnMatches.toMap -- useIndexTables.keySet.map(_.indexColumn)).toList
      val thisFilter = useIndexTables.toList
      val JOINs = joinSQL(superAlias, thisFilter)
      val orderedFilter = thisFilter.map(t => t._1.indexColumn -> t._2) ::: superFilter
      val superSelect = selectBulk(
          superAlias,
          superFilter,
          selectColumns: _*)
      val select =
        superSelect.indexOf("WHERE ") match {
          case -1 => superSelect concat JOINs
          case insPos =>
            superSelect.substring(0, insPos) concat
            JOINs concat
            superSelect.substring(insPos)
        }
      bulkSelect[C, R](orderedFilter, consumer, extractColumns, select, selectColumns.size + 1)
    }

  }

  // private[this] val WHERE_s = WHERE.replace(" version", " s.version")

  protected def joinSQL(mainAlias: Char, tables: List[(IndexTable, QueryValue)]): String = {
    def ON(idxAlias: String) =
      if (version.isDefined) s"ON $idxAlias.version = $mainAlias.version\n  AND" else "ON"

    tables.zipWithIndex.map {
      case ((table, qryVal), n) =>
        val alias = s"i$n"
        val colName = s"$alias.${table.indexColumn}"
        val qryMatch = qryVal.toSQL(colName)

s"""JOIN ${indexTableRef(table)} AS $alias
   ${ON(alias)} $alias.${pkColumn.name} = $mainAlias.${pkColumn.name}
  AND $qryMatch
"""

    }.mkString

  }


  // protected def selectStreamTickSQL(tables: List[(IndexTable, QueryValue)]): String =
  //   selectSQL("s.tick", tables)

  // protected def selectSnapshotSQL(tables: List[(IndexTable, QueryValue)]): String =
  //   selectSQL("s.data, s.revision, s.tick", tables)

  // private[this] val TrueFuture = Future successful Function.const[Boolean, PK](true) _

  // private def query[R](
  //     indexColumnMatches: List[(String, QueryValue)],
  //     superQuery: () => Future[Map[PK, R]],
  //     superKeyQuery: List[(String, QueryValue)] => Future[Set[PK]],
  //     selectSQL: List[(IndexTable, QueryValue)] => String)(
  //     getEntry: ResultSet => (PK, R)): Future[Map[PK, R]] = {

  //   implicit def queryContext = cs.queryContext

  //   val columnMatches = indexColumnMatches.map(e => e._1.toLowerCase -> e._2).toMap
  //   val useIndexTables = columnMatches.toList.flatMap {
  //     case (columnName, matchValue) =>
  //       indexTablesByColumn.get(columnName).map(t => t -> matchValue)
  //   }
  //   if (useIndexTables.isEmpty) {
  //     // No use of index tables, just delegate to super
  //     superQuery()
  //   } else {
  //     // Use index table(s); ensure AND semantics if also using index columns
  //     val useIndexColumns = (columnMatches -- useIndexTables.map(_._1.indexColumn.toLowerCase)).toList
  //     val indexColumnsResult: Future[PK => Boolean] =
  //       if (useIndexColumns.isEmpty) TrueFuture
  //       else superKeyQuery(useIndexColumns)
  //     val indexTablesResult: Future[Map[PK, R]] =
  //       cs.asyncQuery { conn =>
  //         conn.prepare(selectSQL(useIndexTables)) { ps =>
  //           useIndexTables.map(_._2).foldLeft(1) {
  //             case (col, qryVal) =>
  //               qryVal.prepare(ps, col)
  //           }
  //           val rs = ps.executeQuery()
  //           var map = Map.empty[PK, R]
  //           try {
  //             while (rs.next) {
  //               val (stream, value) = getEntry(rs)
  //               map = map.updated(stream, value)
  //             }
  //             map
  //           } finally Try(rs.close)
  //         }
  //       }
  //     for {
  //       indexTablesResult <- indexTablesResult
  //       retainKeys <- indexColumnsResult
  //     } yield {
  //       list.reduce(_ ++ _)
  //         .view.filterKeys(retainKeys).toMap
  //     }
  //   }

  // }

  // override protected def readSnapshots(
  //     indexColumnMatch: (String, QueryValue),
  //     more: (String, QueryValue)*)
  //     : Future[Map[PK, Snapshot]] = {

  //   implicit def queryContext = cs.queryContext

  //   query(
  //     indexColumnMatch :: more.toList,
  //     () => super.readSnapshots(indexColumnMatch, more: _*),
  //     kv => this.readTicks(kv.head, kv.tail: _*).map(_.keySet),
  //     selectSnapshotSQL) { rs =>
  //       val snapshot = this.getSnapshot(rs)(dataColumnType)
  //       val stream = pkColumn.colType.readFrom(rs, 4)
  //       stream -> snapshot
  //     }
  // }

  // override protected def readTicks(
  //     indexColumnMatch: (String, QueryValue),
  //     more: (String, QueryValue)*)
  //     : Future[Map[PK, Long]] = {

  //   implicit def queryContext = cs.queryContext

  //   query(
  //     indexColumnMatch :: more.toList,
  //     () => super.readTicks(indexColumnMatch, more: _*),
  //     kv => super.readTicks(kv.head, kv.tail: _*).map(_.keySet),
  //     selectStreamTickSQL) { rs =>
  //       val tick = rs.getLong(1)
  //       val stream = pkColumn.colType.readFrom(rs, 2)
  //       stream -> tick
  //     }
  // }

  private[this] val WHERE_i = WHERE.replace(" version", " i.version")
  private[this] val ON_io = version.map(_ => s"ON i.version = o.version\n  AND ") getOrElse "ON "
  private[this] val WHERE_d = version.map(v => s"\n  WHERE d.version=$v\n") getOrElse ""

  override protected def selectDuplicatesSQL(indexColumn: String): String =
    indexTablesByColumn.get(indexColumn.toLowerCase) match {
      case None =>
        super.selectDuplicatesSQL(indexColumn)

      case Some(table) =>
        val indexTableRef = this.indexTableRef(table)
s"""
SELECT i.$indexColumn, i.$pkColumnName, o.tick
FROM $indexTableRef AS i
JOIN $tableRef AS o
  $ON_io i.$pkColumnName = o.$pkColumnName
$WHERE_i i.$indexColumn IN (
  SELECT d.$indexColumn
  FROM $indexTableRef AS d$WHERE_d
  GROUP BY d.$indexColumn
  HAVING COUNT(*) > 1
)
"""

    }

}
