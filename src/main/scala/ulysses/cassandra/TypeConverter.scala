package ulysses.cassandra

import scala.reflect.{ ClassTag, classTag }

abstract class TypeConverter[T: ClassTag]
    extends ulysses.conv.TypeConverter[T] {
  final type ROW = com.datastax.driver.core.Row
  final val jvmType: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]
}
