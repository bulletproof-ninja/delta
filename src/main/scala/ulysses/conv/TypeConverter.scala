package ulysses.conv

import scala.reflect.{ ClassTag, classTag }

trait TypeConverter[T] {
  type REC
  type IDX
  def jvmType: Class[T]
  def typeName: String
  def writeAs(value: T): AnyRef = value.asInstanceOf[AnyRef]
  def readFrom(rec: REC, idx: IDX): T
}
