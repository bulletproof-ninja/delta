package ulysses

import java.util.UUID
import scala.reflect._
import com.datastax.driver.core.Row
import java.math.BigInteger
import java.net.InetAddress
import collection.immutable._
import collection.{ Map => aMap, Seq => aSeq, Set => aSet }
import collection.mutable.{ Map => mMap, Seq => mSeq, Set => mSet }

package object cassandra {
  object TimeUUIDConverter extends TypeConverter[UUID] {
    def typeName = "timeuuid"
    def readFrom(row: Row, col: Int): UUID = row.getUUID(col)
  }
  implicit object UUIDConverter extends TypeConverter[UUID] {
    def typeName = "uuid"
    def readFrom(row: Row, col: Int): UUID = row.getUUID(col)
  }
  implicit object LongConverter extends TypeConverter[Long] {
    def typeName = "bigint"
    def readFrom(row: Row, col: Int) = row.getLong(col)
  }
  implicit object IntConverter extends TypeConverter[Int] {
    def typeName = "int"
    def readFrom(row: Row, col: Int) = row.getInt(col)
  }
  implicit object UTF8Converter extends TypeConverter[String] {
    def typeName = "text"
    def readFrom(row: Row, col: Int) = row.getString(col)
  }
  object ASCIIConverter extends TypeConverter[String] {
    def typeName = "ascii"
    def readFrom(row: Row, col: Int) = row.getString(col)
  }
  implicit object BigIntConverter extends TypeConverter[BigInt] {
    def typeName = "varint"
    def readFrom(row: Row, col: Int): BigInt = row.getVarint(col)
    override def writeAs(bint: BigInt) = bint.underlying
  }
  implicit object BlobConverter extends TypeConverter[Array[Byte]] {
    def typeName = "blob"
    def readFrom(row: Row, col: Int): Array[Byte] = row.getBytesUnsafe(col).array()
  }
  implicit object InetAddrConverter extends TypeConverter[InetAddress] {
    def typeName = "inet"
    def readFrom(row: Row, col: Int): InetAddress = row.getInet(col)
  }
  implicit def JavaEnumConverter[T <: java.lang.Enum[T]: ClassTag] =
    new TypeConverter[T] with conv.JavaEnumConverter[T] {
      def typeName = "ascii"
      def readFrom(row: Row, col: Int): T = byName(row.getString(col))
    }
  implicit def ScalaEnumConverter[E <: Enumeration: ClassTag] =
    new TypeConverter[E#Value] with conv.ScalaEnumConverter[E] {
      val enumType = classTag[E].runtimeClass
      def typeName = "ascii"
      def readFrom(row: Row, col: Int) = byName(row.getString(col))
    }
  private abstract class AbstractMapConverter[K: TypeConverter, V: TypeConverter, M <: aMap[K, V]: ClassTag]
      extends TypeConverter[M] {
    import collection.JavaConverters._
    type T = M
    @inline protected def kType = implicitly[TypeConverter[K]]
    @inline protected def vType = implicitly[TypeConverter[V]]
    final val typeName = s"frozen<map<${kType.typeName},${vType.typeName}>>"
    final override def writeAs(map: T): java.util.Map[K, V] = map.asJava
  }

  implicit def MapConverter[K: TypeConverter, V: TypeConverter]: TypeConverter[Map[K, V]] =
    new AbstractMapConverter[K, V, Map[K, V]] {
      import collection.JavaConverters._
      def readFrom(row: Row, col: Int): T =
        row.getMap(col, kType.jvmType, vType.jvmType).asScala.toMap
    }
  implicit def AnyMapConverter[K: TypeConverter, V: TypeConverter]: TypeConverter[aMap[K, V]] =
    new AbstractMapConverter[K, V, aMap[K, V]] {
      import collection.JavaConverters._
      def readFrom(row: Row, col: Int): T =
        row.getMap(col, kType.jvmType, vType.jvmType).asScala
    }
  implicit def MutableMapConverter[K: TypeConverter, V: TypeConverter]: TypeConverter[mMap[K, V]] =
    new AbstractMapConverter[K, V, mMap[K, V]] {
      import collection.JavaConverters._
      def readFrom(row: Row, col: Int): T =
        row.getMap(col, kType.jvmType, vType.jvmType).asScala
    }

  private abstract class AbstractSeqConverter[V: TypeConverter, S <: aSeq[V]: ClassTag]
      extends TypeConverter[S] {
    import collection.JavaConverters._
    type T = S
    @inline protected def vType = implicitly[TypeConverter[V]]
    final val typeName = s"frozen<list<${vType.typeName}>>"
    final override def writeAs(seq: T): java.util.List[V] = seq.asJava
  }

}
