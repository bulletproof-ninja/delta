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
}

object ColumnType {

  def apply[T: ClassTag, SQL: ColumnType](
      codec: Codec[T, SQL]): ColumnType[T] = new ColumnType[T] {
    def typeName = implicitly[ColumnType[SQL]].typeName
    def readFrom(rs: ResultSet, col: Int): T = codec decode {
      implicitly[ColumnType[SQL]].readFrom(rs, col)
    }
    override def writeAs(t: T) = implicitly[ColumnType[SQL]].writeAs(codec encode t)
  }

}
