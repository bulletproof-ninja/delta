package delta.jdbc

import java.sql.ResultSet

package mysql {
  object TextColumn extends ColumnType[String] {
    def typeName = "TEXT"
    def readFrom(rs: ResultSet, col: Int): String = rs.getString(col)
  }
}

package object mysql {

}
