package ulysses.jdbc

import scala.reflect.{ ClassTag, classTag }

abstract class TypeConverter[T: ClassTag]
    extends ulysses.conv.TypeConverter[T] {
  final type ROW = java.sql.ResultSet
  final val jvmType: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]
}
