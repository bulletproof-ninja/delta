package ulysses.conv

import scala.reflect.{ ClassTag, classTag }

trait ColumnType[T] {
  type REC
  def jvmType: Class[T]
  def typeName: String
  def writeAs(value: T): AnyRef = value.asInstanceOf[AnyRef]
  def readFrom(rec: REC, idx: Int): T
}
