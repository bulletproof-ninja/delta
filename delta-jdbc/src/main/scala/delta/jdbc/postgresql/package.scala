package delta.jdbc

import java.sql.ResultSet

package object postgresql {
  object ByteaColumn extends ColumnType[Array[Byte]] {
    def typeName = "bytea"
    def readFrom(row: ResultSet, col: Int): Array[Byte] = row.getBytes(col)
  }
}
