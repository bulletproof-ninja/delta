package delta.jdbc

import java.sql.Connection
import javax.sql.DataSource
import scala.util.Try
import concurrent.blocking

/**
 * Generic trait for providing a JDBC connection.
 * By default, any warnings will get thrown. This
 * behavior can be modified by either fixing the
 * cause of the warning(s), which is the recommended
 * approach, or, if a fix is infeasible, overriding the
 * `processWarnings` method.
 */
trait ConnectionProvider {
  protected def useConnection[R](thunk: Connection => R): R
}

trait DataSourceConnectionProvider extends ConnectionProvider {
  protected def dataSource: DataSource
  protected def useConnection[R](thunk: Connection => R): R = blocking {
    val conn = dataSource.getConnection
    try thunk(conn) finally Try(conn.close)
  }
}

trait PooledConnectionProvider extends ConnectionProvider {
  protected def dataSource: javax.sql.ConnectionPoolDataSource
  protected def useConnection[R](thunk: Connection => R): R = blocking {
    val conn = dataSource.getPooledConnection.getConnection
    try thunk(conn) finally Try(conn.close)
  }
}
