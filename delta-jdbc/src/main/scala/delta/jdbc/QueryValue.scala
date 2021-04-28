package delta.jdbc

import java.sql.PreparedStatement

sealed abstract class QueryValue {
  def toSQL(colName: String): String
  def setValue(ps: PreparedStatement, colOffset: Int): Int
}

final case object NULL
extends QueryValue {
  def toSQL(colName: String) = s"$colName IS NULL"
  def setValue(ps: PreparedStatement, colOffset: Int) =
    colOffset
}

final case object NOT_NULL
extends QueryValue {
  def toSQL(colName: String) = s"$colName IS NOT NULL"
  def setValue(ps: PreparedStatement, colOffset: Int) =
    colOffset
}

final case class EQUALS[T: ColumnType](value: T)
extends QueryValue {
  def toSQL(colName: String) = s"$colName = ?"
  def setValue(ps: PreparedStatement, colOffset: Int) = {
    ps.setValue(colOffset, value)
    colOffset + 1
  }
}

final case class IN[T: ColumnType](values: List[T])
extends QueryValue {
  require(values.nonEmpty && values.tail.nonEmpty, s"Must contain at least two values: $values")
  def this(values: Iterable[T]) = this(values.toList)
  def toSQL(colName: String) = {
    val Qs = values.map(_ => "?").mkString(",")
    s"$colName IN ($Qs)"
  }
  def setValue(ps: PreparedStatement, colOffset: Int) = {
    values.foldLeft(colOffset) {
      case (colOffset, value) =>
        ps.setValue(colOffset, value)
        colOffset + 1
    }
  }

}
