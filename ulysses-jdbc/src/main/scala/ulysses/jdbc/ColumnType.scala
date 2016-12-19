package ulysses.jdbc

import scala.reflect.{ ClassTag, classTag }

abstract class ColumnType[T: ClassTag]
    extends ulysses.conv.StorageType[T] {
  final type Rec = java.sql.ResultSet
  final type Ref = Int
  final val jvmType: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]
  def typeName: String
}
