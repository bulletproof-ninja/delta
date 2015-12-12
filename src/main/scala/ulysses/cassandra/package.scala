package ulysses

import java.util.UUID
import scala.reflect._
import com.datastax.driver.core.Row
import java.math.BigInteger

package object cassandra {
  implicit object UUIDConverter extends TypeConverter[UUID] {
    type CT = UUID
    def cassandraType: ClassTag[CT] = implicitly
    def getValue(row: Row, col: Int) = row.getUUID(col)
    def toCassandraType(uuid: UUID) = uuid
  }
  implicit object LongConverter extends TypeConverter[Long] {
    type CT = Long
    def cassandraType: ClassTag[CT] = implicitly
    def getValue(row: Row, col: Int) = row.getLong(col)
    def toCassandraType(long: Long) = long
  }
  implicit object IntConverter extends TypeConverter[Int] {
    type CT = Int
    def cassandraType: ClassTag[CT] = implicitly
    def getValue(row: Row, col: Int) = row.getInt(col)
    def toCassandraType(int: Int) = int
  }
  implicit object StringConverter extends TypeConverter[String] {
    type CT = String
    def cassandraType: ClassTag[CT] = implicitly
    def getValue(row: Row, col: Int) = row.getString(col)
    def toCassandraType(str: String) = str
  }
  implicit object BigIntConverter extends TypeConverter[BigInt] {
    type CT = BigInteger
    def cassandraType: ClassTag[CT] = implicitly
    def getValue(row: Row, col: Int): BigInt = row.getVarint(col)
    def toCassandraType(bint: BigInt) = bint.underlying
  }
  implicit object BlobConverter extends TypeConverter[Array[Byte]] {
    type CT = Array[Byte]
    def cassandraType: ClassTag[CT] = implicitly
    def getValue(row: Row, col: Int): Array[Byte] = row.getBytes(col).array()
    def toCassandraType(bytes: Array[Byte]) = bytes
  }
  implicit object UnitConverter extends TypeConverter[Unit] {
    type CT = String
    def cassandraType: ClassTag[CT] = implicitly
    def getValue(row: Row, col: Int): Unit = ()
    def toCassandraType(unit: Unit) = "Unit"
  }

  implicit def JavaEnumConverter[T <: java.lang.Enum[T]: ClassTag] = new TypeConverter[T] {
    type CT = String
    def cassandraType: ClassTag[CT] = implicitly
    private[this] val values =
      classTag[T].runtimeClass.getEnumConstants.foldLeft(Map.empty[String, T]) {
        case (map, value) =>
          val t = value.asInstanceOf[T]
          map.updated(t.name, t)
      }
    def getValue(row: Row, col: Int): T = values(row.getString(col))
    def toCassandraType(value: T) = value.name
  }

//  implicit def ScalaEnumConverter[T <: Enumeration#Value: ClassTag] = new TypeConverter[T] {
//    type CT = String
//    private[this] val values = ???
//    def getValue(row: Row, col: Int): T = values(row.getString(col))
//    def toCassandraType(value: T) = value.name
//  }

}
