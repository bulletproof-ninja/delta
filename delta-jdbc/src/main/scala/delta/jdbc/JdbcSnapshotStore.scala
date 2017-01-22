package delta.jdbc

import scuff._
import collection.Map
import java.sql.Connection
import scala.util.Try
import delta.cqrs.ReadModelStore
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import delta.cqrs.ReadModel
import java.sql.PreparedStatement
import java.sql.ResultSet

abstract class AbstractJdbcReadModelStore[K, D: ColumnType] protected (
  table: String, schema: Option[String])(
    implicit jdbcExeCtx: ExecutionContext)
    extends ReadModelStore[K, D] {
  cp: ConnectionProvider =>

  def ensureTable(): Unit =
    useConnection { conn =>
      createTable(conn)
      createIndex(conn)
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

  protected def tickIndexName = tableRef.replace(".", "_") concat "_tick"
  protected def createTickIndexDDL = s"""
    CREATE INDEX IF NOT EXISTS $tickIndexName
      ON $tableRef (tick)
  """.trim

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
  protected def createIndex(conn: Connection): Unit = {
    val stm = conn.createStatement()
    try {
      stm.execute(createTickIndexDDL)
    } finally Try(stm.close)
  }

  def lastTick(): Future[Option[Long]] = Future {
    useConnection { conn =>
      val stm = conn.createStatement()
      try {
        val rs = stm.executeQuery(s"SELECT MAX(tick) FROM $tableRef")
        if (rs.next) {
          val maxTick = rs.getLong(1)
          if (rs.wasNull) None
          else Some(maxTick)
        } else None
      } finally Try(stm.close)
    }
  }

  private def getReadModel(rs: ResultSet): ReadModel[D] = {
    val data = colType[D].readFrom(rs, 1)
    val revision = rs.getInt(2)
    val tick = rs.getLong(3)
    new ReadModel(data, revision, tick)
  }
  private def setReadModel(ps: PreparedStatement, rm: ReadModel[D]): PreparedStatement = {
    ps.setObject(1, colType[D].writeAs(rm.data))
    ps.setInt(2, rm.revision)
    ps.setLong(3, rm.tick)
    ps
  }

  def get(key: K): Future[Option[ReadModel[D]]] = getAll(List(key)).map(_.get(key))
  def getAll(keys: Iterable[K]): Future[Map[K, ReadModel[D]]] = Future {
    useConnection { conn =>
      val ps = conn.prepareStatement(selectOneSQL)
      try {
        keys.flatMap { key =>
          val rs = setKeyParms(ps, key).executeQuery()
          try {
            if (rs.next) Some(key -> getReadModel(rs))
            else None
          } finally Try(rs.close)
        }.toMap
      } finally Try(ps.close)
    }
  }

  def set(key: K, data: ReadModel[D]): Future[Unit] = Future {
    useConnection { conn =>
      set(conn)(key, data)
    }
  }
  private def set(conn: Connection)(key: K, data: ReadModel[D]): Unit = {
    if (data.revision == 0) { // Definitely insert
      insert(conn)(key, data)
    } else { // Who knows? Assume common case, which is update, but insert if not
      update(conn)(key, data) match {
        case 0 => insert(conn)(key, data)
        case 1 => ()
      }
    }
  }
  protected def insert(conn: Connection)(key: K, data: ReadModel[D]): Unit = {
    val ps = conn.prepareStatement(insertOneSQL)
    try {
      setKeyParms(setReadModel(ps, data), key, offset = 3).executeUpdate()
    } finally Try(ps.close)
  }
  protected def update(conn: Connection)(key: K, data: ReadModel[D]): Int = {
    val ps = conn.prepareStatement(updateDataRevTickSQL)
    try {
      setKeyParms(setReadModel(ps, data), key, offset = 3).executeUpdate()
    } finally Try(ps.close)
  }
  def setAll(map: Map[K, ReadModel[D]]): Future[Unit] =
    if (map.isEmpty) Future successful Unit
    else Future {
      useConnection { conn =>
        map.foreach {
          case (key, data) => set(conn)(key, data)
        }
      }
    }
  def update(key: K, revision: Int, tick: Long): Future[Unit] = updateAll(Map(key -> (revision, tick)))
  def updateAll(revisions: Map[K, (Int, Long)]): Future[Unit] =
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

class JdbcReadModelStore1[PK1: ColumnType, D: ColumnType](
  table: String, schema: Option[String] = None)(
    implicit jdbcExeCtx: ExecutionContext)
    extends AbstractJdbcReadModelStore[PK1, D](table, schema) {
  cp: ConnectionProvider =>

  def this(table: String, schema: String)(
    implicit jdbcExeCtx: ExecutionContext) = this(table, schema.optional)

  protected def pk1Name = "pk1"

  protected val pkColumns = List(ColumnDef(pk1Name, colType[PK1].typeName))

  protected def setKeyParms(ps: PreparedStatement, k: PK1, offset: Int): PreparedStatement = {
    ps.setObject(1 + offset, colType[PK1].writeAs(k))
    ps
  }

}

class JdbcReadModelStore2[PK1: ColumnType, PK2: ColumnType, D: ColumnType](
  table: String, schema: Option[String] = None)(
    implicit jdbcExeCtx: ExecutionContext)
    extends AbstractJdbcReadModelStore[(PK1, PK2), D](table, schema) {
  cp: ConnectionProvider =>

  def this(table: String, schema: String)(
    implicit jdbcExeCtx: ExecutionContext) = this(table, schema.optional)

  protected def pk1Name = "pk1"
  protected def pk2Name = "pk2"

  protected val pkColumns = List(
    ColumnDef(pk1Name, colType[PK1].typeName),
    ColumnDef(pk2Name, colType[PK2].typeName))

  protected def setKeyParms(ps: PreparedStatement, k: (PK1, PK2), offset: Int): PreparedStatement = {
    ps.setObject(1 + offset, colType[PK1].writeAs(k._1))
    ps.setObject(2 + offset, colType[PK2].writeAs(k._2))
    ps
  }

}

class JdbcReadModelStore3[PK1: ColumnType, PK2: ColumnType, PK3: ColumnType, D: ColumnType](
  table: String, schema: Option[String] = None)(
    implicit jdbcExeCtx: ExecutionContext)
    extends AbstractJdbcReadModelStore[(PK1, PK2, PK3), D](table, schema) {
  cp: ConnectionProvider =>

  def this(table: String, schema: String)(
    implicit jdbcExeCtx: ExecutionContext) = this(table, schema.optional)

  protected def pk1Name = "pk1"
  protected def pk2Name = "pk2"
  protected def pk3Name = "pk3"

  protected val pkColumns = List(
    ColumnDef(pk1Name, colType[PK1].typeName),
    ColumnDef(pk2Name, colType[PK2].typeName),
    ColumnDef(pk3Name, colType[PK3].typeName))

  protected def setKeyParms(ps: PreparedStatement, k: (PK1, PK2, PK3), offset: Int): PreparedStatement = {
    ps.setObject(1 + offset, colType[PK1].writeAs(k._1))
    ps.setObject(2 + offset, colType[PK2].writeAs(k._2))
    ps.setObject(3 + offset, colType[PK3].writeAs(k._3))
    ps
  }

}
