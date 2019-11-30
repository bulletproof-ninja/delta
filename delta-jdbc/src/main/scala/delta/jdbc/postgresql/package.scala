package delta.jdbc

import java.sql.ResultSet

package postgresql {
  object ByteaColumn extends ColumnType[Array[Byte]] {
    def typeName = "bytea"
    def readFrom(row: ResultSet, col: Int): Array[Byte] = row.getBytes(col)
  }
}

package object postgresql {
  def WithTimestamp(): WithTimestamp = new WithTimestamp(sqlType = "TIMESTAMPTZ")
  def WithTimestamp(colName: String): WithTimestamp = new WithTimestamp(colName = colName, sqlType = "TIMESTAMPTZ")
}