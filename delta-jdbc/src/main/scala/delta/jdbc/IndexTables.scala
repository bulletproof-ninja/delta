package delta.jdbc

import java.sql.Connection
import delta.jdbc.JdbcStreamProcessStore.PkColumn
import scala.util.Try
import java.sql.PreparedStatement
import scala.concurrent.Future
import java.sql.ResultSet

object IndexTables {
  case class Table[S, C: ColumnType](indexColumn: String)(getIndexValues: S => Set[C]) {
    def getIndexValues(state: S): Set[Any] = this.getIndexValues.apply(state).asInstanceOf[Set[Any]]
    def colType = implicitly[ColumnType[C]].asInstanceOf[ColumnType[Any]]
  }
}

/**
 * Enable indexed lookup for many-to-one associations,
 * by creating one or more index tables.
 * NOTE: For simple one-to-one associations,
 * use [[delta.jdbc.JdbcStreamProcessStore.Index]]
 */
trait IndexTables[PK, S, U]
extends JdbcStreamProcessStore[PK, S, U] {

  protected type Table = IndexTables.Table[S, _]
  protected def Table[C: ColumnType](indexColumn: String)(getIndexValues: S => Set[C]): Table =
    IndexTables.Table[S, C](indexColumn)(getIndexValues)

  protected def indexTables: List[Table]
  private lazy val indexTablesByColumn = {
    val map = indexTables.map(t => t.indexColumn.toLowerCase -> t).toMap[String, Table]
    require(map.size == indexTables.size, s"Name clash on index tables (perhaps a casing issue?)")
    map
  }

  protected def indexTableRef(table: Table): String = s"${tableRef}_${table.indexColumn}"

  protected def createIndexTableDDL(table: Table): String = {
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

  protected def deleteRowsSQL(table: Table, deleteCount: Int = 1): String = {
    assert(deleteCount > 0)
    val matchPart = deleteCount match {
      case 1 => "= ?"
      case n => Iterator.fill(n)("?").mkString("IN (", ",", ")")
    }
    s"""
DELETE FROM ${indexTableRef(table)}
$WHERE ${pkColumn.name} = ?
AND ${table.indexColumn} $matchPart
"""
  }
  protected def insertRowSQL(table: Table): String = {
    val (vCol, vQ) = version.map(version => "version, " -> s"$version, ") getOrElse "" -> ""
    s"""
INSERT INTO ${indexTableRef(table)}
($vCol${table.indexColumn}, ${pkColumn.name})
VALUES ($vQ?,?)
"""
  }

  protected def deleteAsBatchThreshold: Int = 10

  private def deleteRowsAsBatch(ps: PreparedStatement, stream: PK, table: Table, keys: Set[Any]): Unit = {
    keys.foreach { key =>
      ps.setValue(1, stream)(pkColumn.colType)
      ps.setValue(2, key)(table.colType)
      ps.addBatch()
    }
    ps.executeBatch()
  }

  private def deleteRowsAsOne(ps: PreparedStatement, stream: PK, table: Table, keys: Set[Any]): Unit = {
    ps.setValue(1, stream)(pkColumn.colType)
    keys.iterator.zipWithIndex.foreach {
      case (key, idx) => ps.setValue(2 + idx, key)(table.colType)
    }
    ps.executeUpdate()
  }

  protected def deleteIndexTableRows(conn: Connection, stream: PK)(table: Table, keys: Set[Any]): Unit = {
    val deleteAsBatch = keys.size >= deleteAsBatchThreshold
    val sql = if (deleteAsBatch) deleteRowsSQL(table) else deleteRowsSQL(table, keys.size)
    val ps = conn.prepareStatement(sql)
    try {
      if (deleteAsBatch) deleteRowsAsBatch(ps, stream, table, keys)
      else deleteRowsAsOne(ps, stream, table, keys)
    } finally {
      Try(ps.close)
    }
  }

  protected def insertIndexTableRows(conn: Connection, stream: PK)(table: Table, keys: Set[Any]): Unit = {
    assert(keys.nonEmpty)
    val isBatch = keys.size > 1
    val ps = conn.prepareStatement(insertRowSQL(table))
    try {
      keys.foreach { key =>
        ps.setValue(1, key)(table.colType)
        ps.setValue(2, stream)(pkColumn.colType)
        if (isBatch) ps.addBatch()
      }
      if (isBatch) ps.executeBatch()
      else ps.executeUpdate()
    } finally {
      Try(ps.close)
    }
  }

  override protected def ensureTable(conn: Connection): Unit = {
    super.ensureTable(conn)
    indexTables.foreach { table =>
      val ddl = createIndexTableDDL(table)
      createTable(conn, ddl)
    }
  }

  override protected def writeIfAbsent(conn: Connection)(
      key: PK, snapshot: Snapshot): Option[Snapshot] = {
    super.writeIfAbsent(conn)(key, snapshot) orElse {
      indexTables.foreach { table =>
        val keyValues = table getIndexValues snapshot.content
        if (keyValues.nonEmpty) insertIndexTableRows(conn, key)(table, keyValues)
      }
      None
    }
  }

  override protected def writeReplacement(conn: Connection)(
      key: PK, oldSnapshot: Snapshot, newSnapshot: Snapshot): Option[Snapshot] = {
    super.writeReplacement(conn)(key, oldSnapshot, newSnapshot) orElse {
      indexTables.foreach { table =>
        val oldValues = table getIndexValues oldSnapshot.content
        val newValues = table getIndexValues newSnapshot.content
        if (oldValues != newValues) {
          val deleteValues = oldValues diff newValues
          if (deleteValues.nonEmpty) deleteIndexTableRows(conn, key)(table, deleteValues)
          val insertValues = newValues diff oldValues
          if (insertValues.nonEmpty) insertIndexTableRows(conn, key)(table, insertValues)
        }
      }
      None
    }
  }

  private[this] val indexTableWHERE = version.map(version => s"WHERE i.version = $version\nAND") getOrElse "WHERE"
  private[this] val joinON = if (version.isDefined) "ON i.version = s.version\n  AND" else "ON"

  protected def selectStreamTickSQL(table: Table): String = {
      def WHERE = indexTableWHERE
      def ON = joinON
    s"""
SELECT s.tick, s.${pkColumn.name}
FROM ${indexTableRef(table)} AS i
JOIN ${tableRef} AS s
  $ON i.${pkColumn.name} = s.${pkColumn.name}
$WHERE i.${table.indexColumn} = ?
"""
  }
  protected def selectSnapshotSQL(table: Table): String = {
      def WHERE = indexTableWHERE
      def ON = joinON
    s"""
SELECT s.data, s.revision, s.tick, s.${pkColumn.name}
FROM ${indexTableRef(table)} AS i
JOIN ${tableRef} AS s
  $ON i.${pkColumn.name} = s.${pkColumn.name}
$WHERE i.${table.indexColumn} = ?
"""
  }

  private[this] val TrueFuture = Future successful Function.const[Boolean, PK](true) _

  private def query[R](
      indexColumnMatches: List[(String, Any)],
      superQuery: () => Future[Map[PK, R]],
      superKeyQuery: List[(String, Any)] => Future[Set[PK]],
      selectSQL: Table => String)(
      getEntry: ResultSet => (PK, R)): Future[Map[PK, R]] = {
    val columnMatches = indexColumnMatches.map(e => e._1.toLowerCase -> e._2).toMap
    val useIndexTables = columnMatches.toList.flatMap {
      case (columnName, matchValue) =>
        indexTablesByColumn.get(columnName).map(t => t -> matchValue)
    }
    if (useIndexTables.isEmpty) {
      // No use of index tables, just delegate to super
      superQuery()
    } else {
      // Use index table(s); ensure AND semantics if also using index columns
      val useIndexColumns = (columnMatches -- useIndexTables.map(_._1.indexColumn.toLowerCase)).toList
      val indexColumnsResult: Future[PK => Boolean] =
        if (useIndexColumns.isEmpty) TrueFuture
        else superKeyQuery(useIndexColumns)
      val indexTablesResult: List[Future[Map[PK, R]]] = useIndexTables.map {
        case (table, matchValue) => futureQuery { conn =>
          val ps = conn.prepareStatement(selectSQL(table))
          try {
            ps.setValue(1, matchValue)(table.colType)
            val rs = ps.executeQuery()
            var map = Map.empty[PK, R]
            try {
              while (rs.next) {
                val (stream, value) = getEntry(rs)
                map = map.updated(stream, value)
              }
              map
            } finally Try(rs.close)
          } finally Try(ps.close)
        }
      }
      for {
        list <- Future.sequence(indexTablesResult)
        retainKeys <- indexColumnsResult
      } yield {
        list.reduce(_ ++ _)
          .filterKeys(retainKeys).toMap
      }
    }

  }

  override protected def querySnapshot(
      indexColumnMatch: (String, Any), more: (String, Any)*): Future[Map[PK, Snapshot]] =
    query(
      indexColumnMatch :: more.toList,
      () => super.querySnapshot(indexColumnMatch, more: _*),
      kv => this.queryTick(kv.head, kv.tail: _*).map(_.keySet),
      selectSnapshotSQL) { rs =>
        val snapshot = this.getSnapshot(rs)(dataColumnType)
        val stream = pkColumn.colType.readFrom(rs, 4)
        stream -> snapshot
      }

  override protected def queryTick(indexColumnMatch: (String, Any), more: (String, Any)*): Future[Map[PK, Long]] =
    query(
      indexColumnMatch :: more.toList,
      () => super.queryTick(indexColumnMatch, more: _*),
      kv => super.queryTick(kv.head, kv.tail: _*).map(_.keySet),
      selectStreamTickSQL) { rs =>
        val tick = rs.getLong(1)
        val stream = pkColumn.colType.readFrom(rs, 2)
        stream -> tick
      }

}
