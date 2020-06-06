package delta.jdbc

/** Column definition for timestamp column. */
case class TimestampColumn(
  columnName: String = TimestampColumn.columnName,
  sqlType: String = TimestampColumn.sqlType,
  sqlFunction: String = TimestampColumn.sqlFunction)

object TimestampColumn
extends TimestampColumn(
  columnName = "updated",
  sqlType = "TIMESTAMP",
  sqlFunction = "NOW()")
