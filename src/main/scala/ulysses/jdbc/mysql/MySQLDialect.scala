package ulysses.jdbc.mysql

import ulysses.jdbc.ColumnType
import ulysses.EventCodec
import java.sql.Connection
import java.sql.SQLException

class MySQLDialect[ID: ColumnType, EVT, CH: ColumnType, SF: ColumnType](_schema: String)(
  implicit codec: EventCodec[EVT, SF])
    extends ulysses.jdbc.Dialect[ID, EVT, CH, SF](_schema) {

  override def channelIndexDDL = super.channelIndexDDL.replace("IF NOT EXISTS ", "")
  override def createChannelIndex(conn: Connection) = {
    try {
      super.createChannelIndex(conn)
    } catch {
      case sqlEx: SQLException if sqlEx.getErrorCode == 1061 => // Already exists
    }
  }
  override def metadataValType = "VARCHAR(32767)"
}
