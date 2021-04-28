package delta.jdbc

import java.sql.ResultSet

package postgresql {

  /** Same as BLOB. */
  object ByteaColumn extends ByteaColumn
  /** Same as BLOB. */
  class ByteaColumn extends ColumnType[Array[Byte]] {
    def typeName = "bytea"
    def readFrom(row: ResultSet, col: Int): Array[Byte] = row getBytes col
  }
  /** Same as CLOB. */
  object TextColumn extends TextColumn
  /** Same as CLOB. */
  class TextColumn extends ColumnType[String] {
    def typeName = "text"
    def readFrom(row: ResultSet, col: Int): String = row getString col
  }
}

package object postgresql {
  def UpdateTimestamp(): UpdateTimestamp =
    new UpdateTimestamp(sqlType = "TIMESTAMPTZ")
  def UpdateTimestamp(columnName: String): UpdateTimestamp =
    new UpdateTimestamp(columnName = columnName, sqlType = "TIMESTAMPTZ")
}
