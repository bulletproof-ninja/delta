package delta.cassandra

import scala.reflect.{ ClassTag, classTag }
import scala.annotation.implicitNotFound

@implicitNotFound("Undefined column type ${T}. An implicit instance of delta.cassandra.ColumnType[${T}] must be in scope")
abstract class ColumnType[T: ClassTag]
    extends delta.conv.StorageType[T] {
  final type Rec = com.datastax.driver.core.Row
  final type Ref = Int
  final val jvmType: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]
  def typeName: String
}
