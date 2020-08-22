package delta.jdbc.mysql

import java.sql.Connection
import java.sql.SQLException
import delta.jdbc.AbstractJdbcStore

/**
 * MySQL syntax adapter for stream process stores.
 */
trait MySQLSyntax {
  self: AbstractJdbcStore =>

  import MySQLDialect._

  override protected def schemaDDL(schema: String): String =
    self.schemaDDL(schema).replace("SCHEMA", "DATABASE")

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
