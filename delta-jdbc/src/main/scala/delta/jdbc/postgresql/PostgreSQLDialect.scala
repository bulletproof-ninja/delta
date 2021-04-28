package delta.jdbc.postgresql

import scuff._
import delta.jdbc._

/**
  * MySQL dialect. Doesn't support schema.
  */
class PostgreSQLDialect[ID: ColumnType, EVT, SF: ColumnType](schema: String = null)
  extends delta.jdbc.Dialect[ID, EVT, SF](schema.optional) {

  override def eventVersionDataType = "SMALLINT"

}
