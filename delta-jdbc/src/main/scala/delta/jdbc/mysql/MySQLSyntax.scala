package delta.jdbc.mysql

import java.sql.Connection
import java.sql.SQLException
import delta.jdbc.AbstractJdbcStore

/**
 * MySQL syntax adapter for stream process stores.
 */
trait MySQLSyntax
extends AbstractJdbcStore {

  import MySQLDialect._

  override protected def schemaDDL(schema: String): String =
    super.schemaDDL(schema).replace("SCHEMA", "DATABASE")

  override protected def createIndex(conn: Connection, ddl: String): Unit = {
    val validDDL = ddl.replace("IF NOT EXISTS ", "")
    try super.createIndex(conn, validDDL) catch {
      case sqlEx: SQLException if isIndexAlreadyExists(sqlEx) => // Ignore
    }
  }

}
