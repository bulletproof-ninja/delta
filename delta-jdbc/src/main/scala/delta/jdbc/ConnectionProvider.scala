package delta.jdbc

import java.sql.{ Connection, SQLWarning }

import scala.concurrent.blocking
import scala.util.Try

import javax.sql.{ DataSource, ConnectionPoolDataSource }

/**
 * Generic trait for providing a JDBC connection.
 * By default, any warnings will get thrown. This
 * behavior can be modified by either fixing the
 * cause of the warning(s), which is the recommended
 * approach, or, if a fix is infeasible, overriding the
 * `processWarnings` method.
 */
trait ConnectionProvider {
  protected def getConnection: Connection
  protected def processWarnings(warnings: SQLWarning): Unit = throw warnings
  protected def useConnection[R](thunk: Connection => R): R = blocking {
    val conn = getConnection
    try {
      val r = thunk(conn)
      Option(conn.getWarnings).foreach(processWarnings)
      r
    } finally Try(conn.close)
  }
}

trait DataSourceConnectionProvider extends ConnectionProvider {
  protected def dataSource: DataSource
  protected def getConnection: Connection = dataSource.getConnection
}

trait PooledConnectionProvider extends ConnectionProvider {
  protected def dataSource: ConnectionPoolDataSource
  protected def getConnection: Connection = dataSource.getPooledConnection.getConnection
}
