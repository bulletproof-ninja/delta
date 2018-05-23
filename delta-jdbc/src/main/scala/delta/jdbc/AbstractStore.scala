package delta.jdbc

import java.sql.{ BatchUpdateException, Connection, PreparedStatement, SQLException }

import scala.util.Try
import java.sql.Statement
import scala.annotation.tailrec
import scala.concurrent._
import scuff.jdbc.ConnectionProvider

abstract class AbstractStore(implicit protected val blockingCtx: ExecutionContext) {
  cp: ConnectionProvider =>

  protected def selectMaxTick(tableRef: String, version: Short) = s"""
SELECT MAX(tick)
FROM $tableRef
WHERE version = $version
  """.trim

  protected def maxTick(tableRef: String, version: Short)(conn: Connection): Option[Long] = {
    val stm = conn.createStatement()
    try {
      val rs = stm executeQuery selectMaxTick(tableRef, version)
      if (rs.next) {
        val tick = rs.getLong(1)
        if (rs.wasNull) None
        else Some(tick)
      } else None
    } finally Try(stm.close)
  }

  protected def futureUpdate[R](thunk: Connection => R): Future[R] = Future(forUpdate(thunk))
  protected def futureQuery[R](thunk: Connection => R): Future[R] = Future(forQuery(thunk))

  /** Execute batch, return failures. */
  protected def executeBatch[K](ps: PreparedStatement, keys: Seq[K]): Seq[K] = {
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
  protected def executeDDL(conn: Connection, ddl: String): Unit = {
    val stm = conn.createStatement()
    try stm.execute(ddl) finally Try(stm.close)
  }
  protected def createTable(conn: Connection, ddl: String): Unit = executeDDL(conn, ddl)
  protected def createIndex(conn: Connection, ddl: String): Unit = executeDDL(conn, ddl)
  protected def dropTable(conn: Connection, ddl: String): Unit = executeDDL(conn, ddl)
  protected def dropIndex(conn: Connection, ddl: String): Unit = executeDDL(conn, ddl)

}
