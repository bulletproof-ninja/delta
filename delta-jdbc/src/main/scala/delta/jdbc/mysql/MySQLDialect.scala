package delta.jdbc.mysql

import java.sql.{ Connection, SQLException }

import delta.jdbc._

/**
  * MySQL dialect. Doesn't support schema.
  */
class MySQLDialect[ID: ColumnType, EVT, CH: ColumnType, SF: ColumnType]
    extends delta.jdbc.Dialect[ID, EVT, CH, SF](None) {

  override val metadataValType = new VarCharColumn(Short.MaxValue).typeName

  override def channelIndexDDL = super.channelIndexDDL.replace("IF NOT EXISTS ", "")
  override def tickIndexDDL = super.channelIndexDDL.replace("IF NOT EXISTS ", "")
  override def eventNameIndexDDL = super.eventNameIndexDDL.replace("IF NOT EXISTS ", "")

  private def ignoreAlreadyExists(thunk: => Unit) = try thunk catch {
    case sqlEx: SQLException if sqlEx.getErrorCode == 1061 => // Already exists, ignore
  }

  override def createChannelIndex(conn: Connection) = ignoreAlreadyExists(super.createChannelIndex(conn))
  override def createTickIndex(conn: Connection) = ignoreAlreadyExists(super.createTickIndex(conn))
  override def createEventNameIndex(conn: Connection) = ignoreAlreadyExists(super.createEventNameIndex(conn))

}
