package ulysses.cassandra

import scala.reflect.{ ClassTag, classTag }

abstract class ColumnType[T: ClassTag]
    extends ulysses.conv.ColumnType[T] {
  final type REC = com.datastax.driver.core.Row
  final val jvmType: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]
}
