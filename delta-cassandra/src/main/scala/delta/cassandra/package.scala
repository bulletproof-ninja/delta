package delta

import java.util.UUID
import scala.reflect._
import com.datastax.driver.core.Row

import java.net.InetAddress
import collection.immutable._
import collection.{ Map => aMap, Seq => aSeq }
import collection.mutable.{ Map => mMap }

package cassandra {
  abstract class ScalaEnumColumn[EV <: Enumeration#Value: ClassTag](val enum: Enumeration)
      extends ColumnType[EV] with conv.ScalaEnumType[EV] {
    def typeName = "ascii"
    def readFrom(row: Row, col: Int) = byName(row.getString(col))
  }

  private abstract class AbstractMapColumn[K: ColumnType, V: ColumnType, M <: aMap[K, V]: ClassTag]
      extends ColumnType[M] {
    import collection.JavaConverters._
    type T = M
    @inline protected def kType = implicitly[ColumnType[K]]
    @inline protected def vType = implicitly[ColumnType[V]]
    final val typeName = s"frozen<map<${kType.typeName},${vType.typeName}>>"
    final override def writeAs(map: T): java.util.Map[K, V] = map.asJava
  }
  private abstract class AbstractSeqColumn[V: ColumnType, S <: aSeq[V]: ClassTag]
      extends ColumnType[S] {
    import collection.JavaConverters._
    type T = S
    @inline protected def vType = implicitly[ColumnType[V]]
    final val typeName = s"frozen<list<${vType.typeName}>>"
    final override def writeAs(seq: T): java.util.List[V] = seq.asJava
  }
}

package object cassandra {
  object TimeUUIDColumn extends ColumnType[UUID] {
    def typeName = "timeuuid"
    def readFrom(row: Row, col: Int): UUID = row.getUUID(col)
  }
  implicit object UUIDColumn extends ColumnType[UUID] {
    def typeName = "uuid"
    def readFrom(row: Row, col: Int): UUID = row.getUUID(col)
  }
  implicit object LongColumn extends ColumnType[Long] {
    def typeName = "bigint"
    def readFrom(row: Row, col: Int) = row.getLong(col)
  }
  implicit object IntColumn extends ColumnType[Int] {
    def typeName = "int"
    def readFrom(row: Row, col: Int) = row.getInt(col)
  }
  implicit object UTF8Column extends ColumnType[String] {
    def typeName = "text"
    def readFrom(row: Row, col: Int) = row.getString(col)
  }
  object ASCIIColumn extends ColumnType[String] {
    def typeName = "ascii"
    def readFrom(row: Row, col: Int) = row.getString(col)
  }
  implicit object BigIntColumn extends ColumnType[BigInt] {
    def typeName = "varint"
    def readFrom(row: Row, col: Int): BigInt = row.getVarint(col)
    override def writeAs(bint: BigInt) = bint.underlying
  }
  implicit object BlobColumn extends ColumnType[Array[Byte]] {
    def typeName = "blob"
    def readFrom(row: Row, col: Int): Array[Byte] = row.getBytesUnsafe(col).array()
  }
  implicit object InetAddrColumn extends ColumnType[InetAddress] {
    def typeName = "inet"
    def readFrom(row: Row, col: Int): InetAddress = row.getInet(col)
  }
  implicit def JavaEnumColumn[T <: java.lang.Enum[T]: ClassTag] =
    new ColumnType[T] with conv.JavaEnumType[T] {
      def typeName = "ascii"
      def readFrom(row: Row, col: Int): T = byName(row.getString(col))
    }

  implicit def MapColumn[K: ColumnType, V: ColumnType]: ColumnType[Map[K, V]] =
    new AbstractMapColumn[K, V, Map[K, V]] {
      import collection.JavaConverters._
      def readFrom(row: Row, col: Int): T =
        row.getMap(col, kType.jvmType, vType.jvmType).asScala.toMap
    }
  implicit def AnyMapColumn[K: ColumnType, V: ColumnType]: ColumnType[aMap[K, V]] =
    new AbstractMapColumn[K, V, aMap[K, V]] {
      import collection.JavaConverters._
      def readFrom(row: Row, col: Int): T =
        row.getMap(col, kType.jvmType, vType.jvmType).asScala
    }
  implicit def MutableMapColumn[K: ColumnType, V: ColumnType]: ColumnType[mMap[K, V]] =
    new AbstractMapColumn[K, V, mMap[K, V]] {
      import collection.JavaConverters._
      def readFrom(row: Row, col: Int): T =
        row.getMap(col, kType.jvmType, vType.jvmType).asScala
    }

  implicit object UnitColumn extends ColumnType[Unit] {
    private[this] val Zero = java.lang.Byte.valueOf(0: Byte)
    def typeName = "tinyint"
    def readFrom(row: Row, col: Int): Unit = ()
    override def writeAs(unit: Unit) = Zero
  }

}
