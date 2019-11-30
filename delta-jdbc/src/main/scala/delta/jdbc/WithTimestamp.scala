package delta.jdbc

/** Column definition for timestamp column, purely for human consumption. */
case class WithTimestamp(colName: String = "updated", sqlType: String = "TIMESTAMP", sqlFunction: String = "NOW()")
