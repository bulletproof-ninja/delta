package delta.jdbc

/** Column definition for timestamp column. */
case class WithTimestamp(
  colName: String = "updated",
  sqlType: String = "TIMESTAMP",
  sqlFunction: String = "NOW()")
