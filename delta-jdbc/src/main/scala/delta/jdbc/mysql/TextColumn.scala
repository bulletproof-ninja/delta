package delta.jdbc.mysql

import delta.jdbc.ColumnType
import java.sql.ResultSet

sealed abstract class TextColumn(
  val typeName: String)
extends ColumnType[String] {

  def readFrom(rs: ResultSet, col: Int): String =
    rs.getString(col)

}

class TextColumn256 extends TextColumn("TINYTEXT")
object TextColumn256 extends TextColumn256

class TextColumn64K extends TextColumn("TEXT")
object TextColumn64K extends TextColumn64K

class TextColumn16M extends TextColumn("MEDIUMTEXT")
object TextColumn16M extends TextColumn16M

// class TextColumn4G extends TextColumn("LONGTEXT")
// object TextColumn4G extends TextColumn4G
