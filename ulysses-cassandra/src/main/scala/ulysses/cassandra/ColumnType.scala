package ulysses.cassandra

import scala.reflect.{ ClassTag, classTag }

abstract class ColumnType[T: ClassTag]
    extends ulysses.conv.StorageType[T] {
  final type Rec = com.datastax.driver.core.Row
  final type Ref = Int
  final val jvmType: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]
  def typeName: String
}
