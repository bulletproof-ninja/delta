package delta.jdbc

import java.sql.Connection
import javax.sql.DataSource
import scala.util.Try

/** Generic trait for providing a JDBC connection. */
trait ConnectionProvider {
  protected def useConnection[R](thunk: Connection => R): R
}

trait DataSourceConnectionProvider extends ConnectionProvider {
  protected def dataSource: DataSource
  protected def useConnection[R](thunk: Connection => R): R = {
    val conn = dataSource.getConnection
    try thunk(conn) finally Try(conn.close)
  }
}

trait PooledConnectionProvider extends ConnectionProvider  {
  protected def dataSource: javax.sql.ConnectionPoolDataSource
  protected def useConnection[R](thunk: Connection => R): R = {
    val conn = dataSource.getPooledConnection.getConnection
    try thunk(conn) finally Try(conn.close)
  }
}
