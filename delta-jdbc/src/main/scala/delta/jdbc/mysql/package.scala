package delta.jdbc

import java.sql.ResultSet

package mysql {
  sealed abstract class TextColumn(val typeName: String)
    extends ColumnType[String] {
    def readFrom(rs: ResultSet, col: Int): String = rs.getString(col)
  }
  object TextColumn256 extends TextColumn("TINYTEXT")
  object TextColumn64K extends TextColumn("TEXT")
  object TextColumn16M extends TextColumn("MEDIUMTEXT")
  object TextColumn4G extends TextColumn("LONGTEXT")
}
