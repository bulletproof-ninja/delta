package delta.jdbc

import scala.reflect.{ ClassTag, classTag }
import scala.annotation.implicitNotFound

@implicitNotFound("Undefined column type ${T}. An implicit instance of delta.jdbc.ColumnType[${T}] must be in scope")
abstract class ColumnType[T: ClassTag]
    extends delta.conv.StorageType[T] {
  final type Rec = java.sql.ResultSet
  final type Ref = Int
  final val jvmType: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]
  def typeName: String
}
