package delta.jdbc

import java.sql.ResultSet

package postgresql {
  object ByteaColumn extends ColumnType[Array[Byte]] {
    def typeName = "bytea"
    def readFrom(row: ResultSet, col: Int): Array[Byte] = row.getBytes(col)
  }
}

package object postgresql {
  def TimestampColumn(): TimestampColumn =
    new TimestampColumn(sqlType = "TIMESTAMPTZ")
  def TimestampColumn(columnName: String): TimestampColumn =
    new TimestampColumn(columnName = columnName, sqlType = "TIMESTAMPTZ")
}
