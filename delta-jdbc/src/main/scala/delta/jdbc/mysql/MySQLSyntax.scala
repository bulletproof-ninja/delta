package delta.jdbc.mysql

import java.sql.Connection
import java.sql.SQLException
import delta.jdbc.AbstractStore

/**
 * MySQL syntax adapter for stream process stores.
 */
trait MySQLSyntax {
  self: AbstractStore =>

  import MySQLDialect._

  override protected def createIndex(conn: Connection, ddl: String): Unit = {
    val validDDL = ddl.replace("IF NOT EXISTS", "")
    try self.createIndex(conn, validDDL) catch {
      case sqlEx: SQLException if isIndexAlreadyExists(sqlEx) => // Ignore
    }
  }
  override protected def dropIndex(conn: Connection, ddl: String): Unit = {
    val validDDL = ddl.replace("IF EXISTS", "")
    try self.dropIndex(conn, validDDL) catch {
      case sqlEx: SQLException if isIndexDoesNotExist(sqlEx) => // Ignore
    }
  }
}
