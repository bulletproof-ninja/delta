package ulysses.jdbc

import scala.reflect.{ ClassTag, classTag }

abstract class ColumnType[T: ClassTag]
    extends ulysses.conv.ColumnType[T] {
  final type REC = java.sql.ResultSet
  final val jvmType: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]
}
