package delta.jdbc.mysql

import java.sql.{ Connection, SQLException }
import delta.jdbc._

/**
  * MySQL dialect.
  * @note Doesn't support schema, because it replaces the
  * selected database.
  */
class MySQLDialect[ID: ColumnType, EVT, SF: ColumnType]
extends delta.jdbc.Dialect[ID, EVT, SF](None) {

  import MySQLDialect._

  override def schemaDDL(schema: String): String = super.schemaDDL(schema).replace("SCHEMA", "DATABASE")

  override def channelIndexDDL = super.channelIndexDDL.replace("IF NOT EXISTS ", "")
  override def tickIndexDDL = super.tickIndexDDL.replace("IF NOT EXISTS ", "")
  override def eventNameIndexDDL = super.eventNameIndexDDL.replace("IF NOT EXISTS ", "")

  private def ignoreAlreadyExists(thunk: => Unit) = try thunk catch {
    case sqlEx: SQLException if isIndexAlreadyExists(sqlEx) => // Already exists, ignore
  }

  override def createChannelIndex(conn: Connection) = ignoreAlreadyExists(super.createChannelIndex(conn))
  override def createTickIndex(conn: Connection) = ignoreAlreadyExists(super.createTickIndex(conn))
  override def createEventNameIndex(conn: Connection) = ignoreAlreadyExists(super.createEventNameIndex(conn))

}

private[mysql] object MySQLDialect {
  def isIndexAlreadyExists(sqlEx: SQLException): Boolean = sqlEx.getErrorCode == 1061
}
