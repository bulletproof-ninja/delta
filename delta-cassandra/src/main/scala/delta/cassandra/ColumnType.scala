package delta.cassandra

import scala.reflect.{ ClassTag, classTag }
import scala.annotation.implicitNotFound
import com.datastax.driver.core.Row
import scuff.Codec

@implicitNotFound("Undefined column type ${T}. An implicit instance of delta.cassandra.ColumnType[${T}] must be in scope")
abstract class ColumnType[T: ClassTag]
extends delta.conv.StorageType[T] {
  final type Rec = Row
  final type Ref = Int
  final val jvmType: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]
  def typeName: String

  def adapt[S: ClassTag](codec: Codec[S, T]): ColumnType[S] =
    ColumnType[S, T](codec)

}

object ColumnType {

  def apply[T: ClassTag, CQL: ColumnType](
      codec: Codec[T, CQL]): ColumnType[T] =
    new ColumnType[T] {
      private[this] val underlying = implicitly[ColumnType[CQL]]

      def typeName =
        underlying.typeName

      def readFrom(row: Row, col: Int): T =
        codec decode underlying.readFrom(row, col)

      override def writeAs(t: T) =
        underlying.writeAs(codec encode t)
    }

}