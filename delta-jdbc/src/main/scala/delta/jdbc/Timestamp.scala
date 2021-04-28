package delta.jdbc

/** Column definition for timestamp column. */
case class UpdateTimestamp(
  columnName: String = UpdateTimestamp.columnName,
  sqlType: String = UpdateTimestamp.sqlType,
  sqlFunction: String = UpdateTimestamp.sqlFunction)

object UpdateTimestamp
extends UpdateTimestamp(
  columnName = "updated",
  sqlType = "TIMESTAMP",
  sqlFunction = "NOW()")
