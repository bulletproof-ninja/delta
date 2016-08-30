package ulysses.jdbc.mysql

import ulysses.jdbc.TypeConverter
import ulysses.EventContext
import java.sql.Connection
import java.sql.SQLException

class MySQLDialect[ID: TypeConverter, EVT, CH: TypeConverter, SF: TypeConverter](_schema: String)(
  implicit evtCtx: EventContext[EVT, CH, SF])
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
