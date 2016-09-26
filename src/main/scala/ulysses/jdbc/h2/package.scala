package ulysses.jdbc

import java.util.UUID
import java.sql.ResultSet

package object h2 {
  implicit object UUIDColumn extends ColumnType[UUID] {
    def typeName = "UUID"
    def readFrom(row: ResultSet, col: Int) = row.getObject(col).asInstanceOf[UUID]
  }
}
