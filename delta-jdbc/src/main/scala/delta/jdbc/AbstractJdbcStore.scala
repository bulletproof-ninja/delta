package delta.jdbc

import delta.Tick

import java.sql.{ BatchUpdateException, Connection, PreparedStatement, SQLException }

import scala.util.Try
import java.sql.Statement
import scala.annotation.tailrec
import scala.concurrent._, duration._

import scuff._
import scuff.concurrent._
import scuff.jdbc.AsyncConnectionSource

abstract class AbstractJdbcStore(
  protected val version: Option[Short],
  protected val tableRef: String,
  schema: Option[String]) {

  protected def connectionSource: AsyncConnectionSource

  protected def schemaDDL(schemaName: String): String = Dialect schemaDDL schemaName

  protected def createTable(conn: Connection): Unit
  protected def createTickIndex(conn: Connection): Unit

  /** Ensure table exists. */
  def ensureTable(ensureTable: Boolean = true): this.type = {
    if (ensureTable) {
      connectionSource.asyncUpdate { conn =>
        this.ensureTable(conn)
      }.await(60.seconds)
    }
    this
  }

  protected def ensureTable(conn: Connection): Unit = {
    schema.foreach { schema =>
      val ddl = schemaDDL(schema)
      createSchema(conn, ddl)
    }
    createTable(conn)
    createTickIndex(conn)
  }

  def name = tableRef

  def tickWatermark: Option[Tick] =
    connectionSource.asyncQuery { conn =>
      maxTick(tableRef)(conn)
    }.await(60.seconds)

  protected def tickIndexName = tableRef.replace(".", "_") concat "_tick"
  protected def createTickIndexDDL = {
    val withVersion = version.map(_ => "version, ") || ""
s"""
CREATE INDEX IF NOT EXISTS $tickIndexName
  ON $tableRef (${withVersion}tick)
"""
  }

  protected def selectMaxTick(tableRef: String) = {
    val WHERE = version.map(version => s"\nWHERE version = $version") || ""
s"""
SELECT MAX(tick)
FROM $tableRef$WHERE
"""
  }

  protected def maxTick(tableRef: String)(conn: Connection): Option[Long] = {
    val stm = conn.createStatement()
    try {
      val rs = stm executeQuery selectMaxTick(tableRef)
      if (rs.next) {
        val tick = rs.getLong(1)
        if (rs.wasNull) None
        else Some(tick)
      } else None
    } finally Try(stm.close)
  }

  protected def futureUpdate[R](thunk: Connection => R): Future[R] = connectionSource.asyncUpdate(thunk)
  protected def futureQuery[R](thunk: Connection => R): Future[R] = connectionSource.asyncQuery(thunk)

  /** Execute batch, return failures. */
  protected def executeBatch[K](ps: PreparedStatement, keys: Iterable[K]): Iterable[K] = {
      @tailrec
      def normalize(arr: Array[Int], idx: Int = 0): Array[Int] =
        if (idx == arr.length) arr
        else {
          arr(idx) match {
            case 0 | 1 => // Expected
            case Statement.SUCCESS_NO_INFO => arr(idx) = 1
            case negative if negative < 0 => arr(idx) = 0
            case _ => arr(idx) = 1 // Should not happen
          }
          normalize(arr, idx + 1)
        }

    val updateStatus = try ps.executeBatch() catch {
      case e: BatchUpdateException =>
        val updateCounts = Option(e.getUpdateCounts).map(normalize(_)) || new Array[Int](keys.size)
        if (updateCounts.length < keys.size) java.util.Arrays.copyOf(updateCounts, keys.size)
        else updateCounts
    }
    (keys zip updateStatus).withFilter(_._2 == 0).map(_._1)
  }

  protected def isDuplicateKeyViolation(sqlEx: SQLException): Boolean = Dialect.isDuplicateKeyViolation(sqlEx)

  protected def createSchema(conn: Connection, ddl: String): Unit = Dialect.executeDDL(conn, ddl)
  protected def createTable(conn: Connection, ddl: String): Unit = Dialect.executeDDL(conn, ddl)
  protected def createIndex(conn: Connection, ddl: String): Unit = Dialect.executeDDL(conn, ddl)
  // protected def dropTable(conn: Connection, ddl: String): Unit = Dialect.executeDDL(conn, ddl)
  // protected def dropIndex(conn: Connection, ddl: String): Unit = Dialect.executeDDL(conn, ddl)

}
