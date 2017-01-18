package delta.jdbc.mysql

import java.sql.{ Connection, SQLException }
import scuff._
import delta.jdbc._

/**
  * MySQL dialect. Doesn't support schema.
  */
class MySQLDialect[ID: ColumnType, EVT, CH: ColumnType, SF: ColumnType](schema: String = null)
    extends delta.jdbc.Dialect[ID, EVT, CH, SF](schema.optional) {

  import MySQLDialect._

  override def schemaDDL(schema: String): String = super.schemaDDL(schema).replace("SCHEMA", "DATABASE")

  override def channelIndexDDL = super.channelIndexDDL.replace("IF NOT EXISTS ", "")
  override def tickIndexDDL = super.channelIndexDDL.replace("IF NOT EXISTS ", "")
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
