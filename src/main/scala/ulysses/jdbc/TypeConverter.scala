package ulysses.jdbc

import java.sql.ResultSet

import scala.reflect.ClassTag

trait TypeConverter[T] {
  type JT
  def jdbcType: ClassTag[JT]
  def getValue(row: ResultSet, column: Int): T
  def toJdbcType(t: T): JT
}
