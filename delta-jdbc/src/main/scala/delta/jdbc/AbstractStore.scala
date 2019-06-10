package delta.jdbc

import java.sql.{ BatchUpdateException, Connection, PreparedStatement, SQLException }

import scala.util.Try
import java.sql.Statement
import scala.annotation.tailrec
import scala.concurrent._
import scuff.jdbc.ConnectionSource

abstract class AbstractStore(
    cs: ConnectionSource,
    protected val version: Option[Short],
    table: String, schema: Option[String])(
    implicit
    protected val blockingCtx: ExecutionContext) {

  protected def createTable(conn: Connection): Unit
  protected def createTickIndex(conn: Connection): Unit

  /** Ensure table. */
  def ensureTable(): this.type =
    cs.forUpdate { conn =>
      ensureTable(conn)
      this
    }

  protected def ensureTable(conn: Connection): Unit = {
    createTable(conn)
    createTickIndex(conn)
  }

  private[this] val schemaRef = schema.map(_ + ".") getOrElse ""
  protected val tableRef: String = schemaRef concat table

  def tickWatermark: Option[Long] =
    cs.forQuery { conn =>
      maxTick(tableRef)(conn)
    }

  protected def tickIndexName = tableRef.replace(".", "_") concat "_tick"
  protected def createTickIndexDDL = {
    val withVersion = version.map(_ => "version, ") getOrElse ""
    s"""
CREATE INDEX IF NOT EXISTS $tickIndexName
  ON $tableRef (${withVersion}tick)
"""
  }

  protected def selectMaxTick(tableRef: String) = s"""
SELECT MAX(tick)
FROM $tableRef
""" concat (version.map(version => s"WHERE version = $version") getOrElse "")

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

  protected def futureUpdate[R](thunk: Connection => R): Future[R] = Future(cs.forUpdate(thunk))
  protected def futureQuery[R](thunk: Connection => R): Future[R] = Future(cs.forQuery(thunk))

  /** Execute batch, return failures. */
  protected def executeBatch[K](ps: PreparedStatement, keys: Iterable[K]): Iterable[K] = {
      @tailrec
      def normalize(arr: Array[Int], idx: Int = 0): Array[Int] = {
        if (idx < arr.length) {
          arr(idx) match {
            case 0 | 1 => // Expected
            case Statement.SUCCESS_NO_INFO => arr(idx) = 1
            case negative if negative < 0 => arr(idx) = 0
            case _ => arr(idx) = 1 // Should not happen
          }
          normalize(arr, idx + 1)
        } else arr
      }
    val updateStatus = try ps.executeBatch() catch {
      case be: BatchUpdateException =>
        val updateCounts = Option(be.getUpdateCounts).map(normalize(_)) getOrElse new Array[Int](keys.size)
        if (updateCounts.length < keys.size) java.util.Arrays.copyOf(updateCounts, keys.size)
        else updateCounts
    }
    (keys zip updateStatus).withFilter(_._2 == 0).map(_._1)
  }

  protected def isDuplicateKeyViolation(sqlEx: SQLException): Boolean = Dialect.isDuplicateKeyViolation(sqlEx)

  import Dialect.executeDDL

  protected def createTable(conn: Connection, ddl: String): Unit = executeDDL(conn, ddl)
  protected def createIndex(conn: Connection, ddl: String): Unit = executeDDL(conn, ddl)
  protected def dropTable(conn: Connection, ddl: String): Unit = executeDDL(conn, ddl)
  protected def dropIndex(conn: Connection, ddl: String): Unit = executeDDL(conn, ddl)

}
