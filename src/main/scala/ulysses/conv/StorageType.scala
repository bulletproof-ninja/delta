package ulysses.conv

import scala.reflect.{ ClassTag, classTag }

trait StorageType[T] {
  type Rec
  type Ref
  def jvmType: Class[T]
  def writeAs(value: T): AnyRef = value.asInstanceOf[AnyRef]
  def readFrom(rec: Rec, ref: Ref): T
}
