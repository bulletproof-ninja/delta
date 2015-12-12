package ulysses

import java.util.UUID
import scala.reflect._
import java.math.BigInteger
import java.sql.ResultSet
import scuff.Numbers._

package object jdbc {
  implicit object UUIDConverter extends TypeConverter[UUID] {
    type JT = Array[Byte]
    def jdbcType: ClassTag[JT] = implicitly
    def getValue(row: ResultSet, col: Int) = {
      val bytes = row.getBytes(col)
      val msb = bytesToLong(bytes, 0)
      val lsb = bytesToLong(bytes, 8)
      new UUID(msb, lsb)
    }
    def toJdbcType(uuid: UUID): Array[Byte] = {
      val bytes = new Array[Byte](16)
      longToBytes(uuid.getMostSignificantBits, bytes, 0)
      longToBytes(uuid.getLeastSignificantBits, bytes, 8)
    }
  }
  implicit object LongConverter extends TypeConverter[Long] {
    type JT = Long
    def jdbcType: ClassTag[JT] = implicitly
    def getValue(row: ResultSet, col: Int) = row.getLong(col)
    def toJdbcType(long: Long): JT = long
  }
  implicit object IntConverter extends TypeConverter[Int] {
    type JT = Int
    def jdbcType: ClassTag[JT] = implicitly
    def getValue(row: ResultSet, col: Int) = row.getInt(col)
    def toJdbcType(int: Int) = int
  }
  implicit object StringConverter extends TypeConverter[String] {
    type JT = String
    def jdbcType: ClassTag[JT] = implicitly
    def getValue(row: ResultSet, col: Int) = row.getString(col)
    def toJdbcType(str: String): JT = str
  }
  implicit object BigIntConverter extends TypeConverter[BigInt] {
    type JT = BigDecimal
    def jdbcType: ClassTag[JT] = implicitly
    def getValue(row: ResultSet, col: Int): BigInt = row.getBigDecimal(col).toBigInteger
    def toJdbcType(bint: BigInt): JT = new java.math.BigDecimal(bint.underlying)
  }
  implicit object BlobConverter extends TypeConverter[Array[Byte]] {
    type JT = Array[Byte]
    def jdbcType: ClassTag[JT] = implicitly
    def getValue(row: ResultSet, col: Int): Array[Byte] = row.getBytes(col)
    def toJdbcType(bytes: Array[Byte]) = bytes
  }
  implicit object UnitConverter extends TypeConverter[Unit] {
    type JT = String
    def jdbcType: ClassTag[JT] = implicitly
    def getValue(row: ResultSet, col: Int): Unit = ()
    def toJdbcType(unit: Unit) = "Unit"
  }

  implicit def JavaEnumConverter[T <: java.lang.Enum[T]: ClassTag] = new TypeConverter[T] {
    type JT = String
    def jdbcType: ClassTag[JT] = implicitly
    private[this] val values =
      classTag[T].runtimeClass.getEnumConstants.foldLeft(Map.empty[String, T]) {
        case (map, value) =>
          val t = value.asInstanceOf[T]
          map.updated(t.name, t)
      }
    def getValue(row: ResultSet, col: Int): T = values(row.getString(col))
    def toJdbcType(value: T) = value.name
  }

}
