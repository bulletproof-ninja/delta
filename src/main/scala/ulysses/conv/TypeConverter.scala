package ulysses.conv

import scala.reflect.{ ClassTag, classTag }

trait TypeConverter[T] {
  type ROW
  def jvmType: Class[T]
  def typeName: String
  def writeAs(value: T): AnyRef = value.asInstanceOf[AnyRef]
  def readFrom(row: ROW, columnIdx: Int): T
}
