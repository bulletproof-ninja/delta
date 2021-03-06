package delta.jdbc

import scala.reflect.{ ClassTag, classTag }
import scala.annotation.implicitNotFound
import scuff.Codec
import java.sql.ResultSet

abstract class ReadColumn[T]
extends delta.conv.ReadFromStorage[T] {
  type Rec = ResultSet
  type Ref = Int

  def readFrom(rs: ResultSet, col: Int): T
}

@implicitNotFound("Undefined column type ${T}. An implicit instance of delta.jdbc.ColumnType[${T}] must be in scope")
abstract class ColumnType[T: ClassTag]
extends ReadColumn[T]
with delta.conv.StorageType[T] {
  final val jvmType: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]
  def typeName: String
  def adapt[S: ClassTag](codec: Codec[S, T]): ColumnType[S] =
    ColumnType[S, T](codec)
}

object ColumnType {

  def apply[T: ClassTag, SQL: ColumnType](
      codec: Codec[T, SQL]): ColumnType[T] =
    new ColumnType[T] {
      private[this] val underlying = implicitly[ColumnType[SQL]]

      def typeName =
        underlying.typeName

      def readFrom(rs: ResultSet, col: Int): T =
        codec decode underlying.readFrom(rs, col)

      override def writeAs(t: T) =
        underlying.writeAs(codec encode t)
    }

}
