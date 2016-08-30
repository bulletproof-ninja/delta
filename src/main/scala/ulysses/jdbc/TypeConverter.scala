package ulysses.jdbc

import scala.reflect.{ ClassTag, classTag }

abstract class TypeConverter[T: ClassTag]
    extends ulysses.conv.TypeConverter[T] {
  final type REC = java.sql.ResultSet
  final type IDX = Int
  final val jvmType: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]
}
