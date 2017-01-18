package delta.jdbc.mysql

import delta.jdbc.AbstractJdbcReadModelStore
import java.sql.Connection
import java.sql.SQLException

trait MySQLReadModelStore {
  store: AbstractJdbcReadModelStore[_, _] =>

  override def createTickIndexDDL: String = store.createTickIndexDDL.replace("IF NOT EXISTS", "")
  override def createIndex(conn: Connection): Unit = {
    try store.createIndex(conn) catch {
      case sqlEx: SQLException if MySQLDialect.isIndexAlreadyExists(sqlEx) => // Ignore
    }
  }
}