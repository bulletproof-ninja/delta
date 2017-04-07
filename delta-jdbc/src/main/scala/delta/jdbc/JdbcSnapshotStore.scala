package delta.jdbc

import scuff._
import collection.Map
import java.sql.Connection
import scala.util.Try
import scala.concurrent.{ Future, ExecutionContext }
import java.sql.PreparedStatement
import java.sql.ResultSet
import delta.SnapshotStore
import delta.Snapshot

abstract class AbstractJdbcSnapshotStore[K, D: ColumnType] protected (
  table: String, schema: Option[String])(
    implicit jdbcCtx: ExecutionContext)
    extends SnapshotStore[K, D] {
  cp: ConnectionProvider =>

  def ensureTable(dropIfExists: Boolean = false): this.type =
    useConnection { conn =>
      if (dropIfExists) {
        dropTable(conn)
      }
      createTable(conn)
      createIndex(conn)
      this
    }

  protected case class ColumnDef(name: String, colTypeName: String)

  protected def colType[T: ColumnType] = implicitly[ColumnType[T]]

  private[this] val schemaRef = schema.map(_ concat ".") getOrElse ""
  protected def tableRef: String = schemaRef concat table
  protected def pkColumns: Seq[ColumnDef]

  protected def createTableDDL = {
    val pkColumnDefs = pkColumns.map {
      case ColumnDef(name, colTypeName) => s"$name $colTypeName NOT NULL"
    }.mkString(",")

    val pkNames = pkColumns.map(_.name).mkString(",")

    s"""CREATE TABLE IF NOT EXISTS $tableRef (
      $pkColumnDefs,
      data ${colType[D].typeName} NOT NULL,
      revision INT NOT NULL,
      tick BIGINT NOT NULL,

      PRIMARY KEY ($pkNames)
    )"""
  }

  protected def dropTableDDL = s"DROP TABLE IF EXISTS $tableRef"

  protected def tickIndexName = tableRef.replace(".", "_") concat "_tick"
  protected def createTickIndexDDL = s"""
    CREATE INDEX IF NOT EXISTS $tickIndexName
      ON $tableRef (tick)
  """.trim
  protected def dropTickIndexDDL = s"DROP INDEX $tickIndexName"

  protected def selectOneSQL = {
    val pkNamesEqual = pkColumns.map { cd =>
      s"${cd.name} = ?"
    }.mkString(" AND ")
    s"""
    SELECT data, revision, tick
    FROM $tableRef
    WHERE $pkNamesEqual
  """
  }
  protected def selectMaxTick = s"""
    SELECT MAX(tick)
    FROM $tableRef
  """
  protected def insertOneSQL = {
    val pkNames = pkColumns.map(_.name).mkString(",")
    val pkQs = Iterable.fill(pkColumns.size)("?").mkString(",")
    s"""
    INSERT INTO $tableRef
    (data, revision, tick, $pkNames)
    VALUES (?,?,?,$pkQs)
    """
  }
  protected def updateDataRevTickSQL = {
    val pkNamesEqual = pkColumns.map { cd =>
      s"${cd.name} = ?"
    }.mkString(" AND ")
    s"""
    UPDATE $tableRef
    SET data = ?, revision = ?, tick = ?
    WHERE $pkNamesEqual
    """
  }
  protected def updateRevTickSQL = {
    val pkNamesEqual = pkColumns.map { cd =>
      s"${cd.name} = ?"
    }.mkString(" AND ")
    s"""
    UPDATE $tableRef
    SET revision = ?, tick = ?
    WHERE $pkNamesEqual
    """
  }

  protected def setKeyParms(ps: PreparedStatement, k: K, offset: Int = 0): PreparedStatement

  protected def createTable(conn: Connection): Unit = {
    val stm = conn.createStatement()
    try {
      stm.execute(createTableDDL)
    } finally Try(stm.close)
  }
  protected def dropTable(conn: Connection): Unit = {
    val stm = conn.createStatement()
    try {
      stm.execute(dropTableDDL)
    } finally Try(stm.close)
  }
  protected def createIndex(conn: Connection): Unit = {
    val stm = conn.createStatement()
    try {
      stm.execute(createTickIndexDDL)
    } finally Try(stm.close)
  }

  private def getSnapshot(rs: ResultSet): Snapshot[D] = {
    val data = colType[D].readFrom(rs, 1)
    val revision = rs.getInt(2)
    val tick = rs.getLong(3)
    new Snapshot(data, revision, tick)
  }
  private def setSnapshot(ps: PreparedStatement, s: Snapshot[D]): PreparedStatement = {
    ps.setObject(1, colType[D].writeAs(s.content))
    ps.setInt(2, s.revision)
    ps.setLong(3, s.tick)
    ps
  }

  def maxTick: Future[Option[Long]] = Future {
    useConnection { conn =>
      val ps = conn.prepareStatement(selectMaxTick)
      try {
        val rs = ps.executeQuery()
        if (rs.next) {
          val tick = rs.getLong(1)
          if (rs.wasNull) None
          else Some(tick)
        } else None
      } finally Try(ps.close)
    }
  }

  def read(key: K): Future[Option[Snapshot[D]]] = readBatch(List(key)).map(_.get(key))
  def readBatch(keys: Iterable[K]): Future[Map[K, Snapshot[D]]] = Future {
    useConnection { conn =>
      val ps = conn.prepareStatement(selectOneSQL)
      try {
        keys.foldLeft(Map.empty[K, Snapshot[D]]) {
          case (map, key) =>
            val rs = setKeyParms(ps, key).executeQuery()
            try {
              if (rs.next) {
                map.updated(key, getSnapshot(rs))
              } else map
            } finally Try(rs.close)
        }
      } finally Try(ps.close)
    }
  }

  def write(key: K, data: Snapshot[D]): Future[Unit] = Future {
    useConnection { conn =>
      set(conn)(key, data)
    }
  }
  private def set(conn: Connection)(key: K, data: Snapshot[D]): Unit = {
    if (data.revision == 0) { // Definitely insert
      insert(conn)(key, data)
    } else { // Who knows? Assume common case, which is update, but insert if not
      update(conn)(key, data) match {
        case 0 => insert(conn)(key, data)
        case 1 => ()
      }
    }
  }
  protected def insert(conn: Connection)(key: K, data: Snapshot[D]): Unit = {
    val ps = conn.prepareStatement(insertOneSQL)
    try {
      setKeyParms(setSnapshot(ps, data), key, offset = 3).executeUpdate()
    } finally Try(ps.close)
  }
  protected def update(conn: Connection)(key: K, data: Snapshot[D]): Int = {
    val ps = conn.prepareStatement(updateDataRevTickSQL)
    try {
      setKeyParms(setSnapshot(ps, data), key, offset = 3).executeUpdate()
    } finally Try(ps.close)
  }
  def writeBatch(map: Map[K, Snapshot[D]]): Future[Unit] =
    if (map.isEmpty) Future successful Unit
    else Future {
      useConnection { conn =>
        map.foreach {
          case (key, data) => set(conn)(key, data)
        }
      }
    }
  def refresh(key: K, revision: Int, tick: Long): Future[Unit] =
    refreshBatch(Map(key -> ((revision, tick))))
  def refreshBatch(revisions: Map[K, (Int, Long)]): Future[Unit] =
    if (revisions.isEmpty) Future successful Unit
    else Future {
      useConnection { conn =>
        val ps = conn.prepareStatement(updateRevTickSQL)
        try {
          val isBatch = revisions.size != 1
          revisions.foreach {
            case (key, (revision, tick)) =>
              ps.setInt(1, revision)
              ps.setLong(2, tick)
              setKeyParms(ps, key, offset = 2)
              if (isBatch) ps.addBatch()
          }
          if (isBatch) ps.executeBatch()
          else ps.executeUpdate()
        } finally Try(ps.close)
      }
    }

}

class JdbcSnapshotStore[PK: ColumnType, D: ColumnType](
  table: String, schema: Option[String] = None)(
    implicit jdbcCtx: ExecutionContext)
    extends AbstractJdbcSnapshotStore[PK, D](table, schema) {
  cp: ConnectionProvider =>

  def this(table: String, schema: String)(
    implicit jdbcExeCtx: ExecutionContext) = this(table, schema.optional)

  protected def pkName = "id"

  protected val pkColumns = List(ColumnDef(pkName, colType[PK].typeName))

  protected def setKeyParms(ps: PreparedStatement, k: PK, offset: Int): PreparedStatement = {
    ps.setObject(1 + offset, colType[PK].writeAs(k))
    ps
  }

}

class JdbcSnapshotStore2[PK1: ColumnType, PK2: ColumnType, D: ColumnType](
  table: String, schema: Option[String] = None)(
    implicit jdbcCtx: ExecutionContext)
    extends AbstractJdbcSnapshotStore[(PK1, PK2), D](table, schema) {
  cp: ConnectionProvider =>

  def this(table: String, schema: String)(
    implicit jdbcCtx: ExecutionContext) = this(table, schema.optional)

  protected def pk1Name = "id_1"
  protected def pk2Name = "id_2"

  protected val pkColumns = List(
    ColumnDef(pk1Name, colType[PK1].typeName),
    ColumnDef(pk2Name, colType[PK2].typeName))

  protected def setKeyParms(ps: PreparedStatement, k: (PK1, PK2), offset: Int): PreparedStatement = {
    ps.setObject(1 + offset, colType[PK1].writeAs(k._1))
    ps.setObject(2 + offset, colType[PK2].writeAs(k._2))
    ps
  }

}
