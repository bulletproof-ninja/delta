package ulysses

import java.util.UUID
import scala.reflect._
import java.math.BigInteger
import java.sql.ResultSet
import scuff.Numbers._

package object jdbc {
  object UUIDBinaryConverter extends UUIDBinaryConverter
  class UUIDBinaryConverter extends TypeConverter[UUID] {
    def typeName = "BINARY(16)"
    def writeAs(uuid: UUID): Array[Byte] = {
      val bytes = new Array[Byte](16)
      longToBytes(uuid.getMostSignificantBits, bytes, 0)
      longToBytes(uuid.getLeastSignificantBits, bytes, 8)
    }
    def readFrom(row: ResultSet, col: Int) = {
      val bytes = row.getBytes(col)
      val msb = bytesToLong(bytes, 0)
      val lsb = bytesToLong(bytes, 8)
      new UUID(msb, lsb)
    }
  }
  object UUIDCharConverter extends UUIDCharConverter
  class UUIDCharConverter extends TypeConverter[UUID] {
    def typeName = "CHAR(36)"
    def writeAs(uuid: UUID): String = uuid.toString
    def readFrom(row: ResultSet, col: Int) = UUID fromString row.getString(col)
  }
  implicit object LongConverter extends TypeConverter[Long] {
    def typeName = "BIGINT"
    def readFrom(row: ResultSet, col: Int) = row.getLong(col)
  }
  implicit object IntConverter extends TypeConverter[Int] {
    def typeName = "INT"
    def readFrom(row: ResultSet, col: Int) = row.getInt(col)
  }
  implicit object StringConverter extends TypeConverter[String] {
    def typeName = "VARCHAR"
    def readFrom(row: ResultSet, col: Int) = row.getString(col)
  }
  implicit object BigIntConverter extends TypeConverter[BigInt] {
    def typeName = "NUMERIC"
    def readFrom(row: ResultSet, col: Int): BigInt = row.getBigDecimal(col).toBigInteger
    def writeAs(bint: BigInt) = new java.math.BigDecimal(bint.underlying)
  }
  implicit object UnitConverter extends TypeConverter[Unit] {
    def typeName = "TINYINT"
    def readFrom(row: ResultSet, col: Int): Unit = ()
    def writeAs(unit: Unit): Byte = 0
  }
  implicit def JavaEnumConverter[T <: java.lang.Enum[T]: ClassTag] =
    new TypeConverter[T] with conv.JavaEnumConverter[T] {
      def typeName = "VARCHAR"
      def readFrom(row: ResultSet, col: Int) = byName(row.getString(col))
    }
  implicit def ScalaEnumConverter[E <: Enumeration: ClassTag] =
    new TypeConverter[E#Value] with conv.ScalaEnumConverter[E] {
      val enumType = classTag[E].runtimeClass
      def typeName = "VARCHAR"
      def readFrom(row: ResultSet, col: Int) = byName(row.getString(col))
    }
}
