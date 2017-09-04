package delta.jdbc

import scuff._
import collection.Map
import java.sql._
import scala.util.Try
import scala.concurrent.{ Future, ExecutionContext }
import delta._
import scuff.jdbc.ConnectionProvider

abstract class AbstractJdbcSnapshotStore[K, D: ColumnType] protected (
  table: String, schema: Option[String])(
    implicit jdbcCtx: ExecutionContext)
    extends SnapshotStore[K, D] {
  cp: ConnectionProvider =>

  def ensureTable(dropIfExists: Boolean = false): this.type =
    forUpdate { conn =>
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
    AND revision <= ? AND tick <= ?
    """
  }
  protected def refreshRevTickSQL = {
    val pkNamesEqual = pkColumns.map { cd =>
      s"${cd.name} = ?"
    }.mkString(" AND ")
    s"""
    UPDATE $tableRef
    SET revision = ?, tick = ?
    WHERE $pkNamesEqual
    AND revision <= ? AND tick <= ?
    """
  }

  protected def setKeyParms(ps: PreparedStatement, k: K, offset: Int = 0): Int

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
  private def setSnapshot(ps: PreparedStatement, s: Snapshot[D]): Int = {
    ps.setObject(1, colType[D].writeAs(s.content))
    ps.setInt(2, s.revision)
    ps.setLong(3, s.tick)
    3
  }
  private def setRevTick(ps: PreparedStatement, rev: Int, tick: Long, offset: Int): Unit = {
    ps.setInt(1 + offset, rev)
    ps.setLong(2 + offset, tick)
  }

  def maxTick: Future[Option[Long]] = Future {
    forQuery { conn =>
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
    forQuery { conn =>
      getAll(conn)(keys)
    }
  }

  private def getAll(conn: Connection)(keys: Iterable[K], map: Map[K, Snapshot[D]] = Map.empty): Map[K, Snapshot[D]] = {
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

  def write(key: K, data: Snapshot[D]): Future[Unit] = Future {
    forUpdate { conn =>
      set(conn)(key, data)
    }
  }
  private def set(conn: Connection)(key: K, data: Snapshot[D]): Unit = {
    // Assume common case, which is update, but insert if not
    update(conn)(key, data) match {
      case 0 => // Update failed; can have 2 reasons: 1) Row doesn't exist yet (thus try insert), 2) Old revision and/or tick
        if (!insert(conn)(key, data)) { // Could be race condition. Super unlikely, but try update again
          if (update(conn)(key, data) == 0) { // At this point, it's probably revision and/or tick
            getAll(conn)(Iterable(key)).get(key) match {
              case Some(existing) =>
                throw SnapshotStore.Exceptions.writeOlderRevision(key, existing, data)
              case None =>
                sys.error(s"Failed to both insert and update for key $key, for unknown reason")
            }

          }
        }
      case 1 => ()
    }
  }

  protected def isDuplicateKeyViolation(sqlEx: SQLException): Boolean = Dialect.isDuplicateKeyViolation(sqlEx)

  protected def insert(conn: Connection)(key: K, data: Snapshot[D]): Boolean = {
    val ps = conn.prepareStatement(insertOneSQL)
    try {
      val offset = setSnapshot(ps, data)
      setKeyParms(ps, key, offset)
      ps.executeUpdate() != 0
    } catch {
      case e: SQLException if isDuplicateKeyViolation(e) =>
        false
    } finally Try(ps.close)
  }
  protected def update(conn: Connection)(key: K, data: Snapshot[D]): Int = {
    val ps = conn.prepareStatement(updateDataRevTickSQL)
    try {
      val snapshotColOffset = setSnapshot(ps, data)
      val offset = setKeyParms(ps, key, snapshotColOffset)
      setRevTick(ps, data.revision, data.tick, offset)
      ps.executeUpdate()
    } finally Try(ps.close)
  }
  def writeBatch(map: Map[K, Snapshot[D]]): Future[Unit] =
    if (map.isEmpty) Future successful Unit
    else Future {
      forUpdate { conn =>
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
      forUpdate { conn =>
        val ps = conn.prepareStatement(refreshRevTickSQL)
        try {
          val isBatch = revisions.size != 1
          revisions.foreach {
            case (key, (revision, tick)) =>
              setRevTick(ps, revision, tick, offset = 0)
              val offset = setKeyParms(ps, key, offset = 2)
              setRevTick(ps, revision, tick, offset = offset)
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

  protected def setKeyParms(ps: PreparedStatement, k: PK, offset: Int): Int = {
    ps.setObject(1 + offset, colType[PK].writeAs(k))
    offset + 1
  }

}
