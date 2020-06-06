package delta.conv

trait ReadFromStorage[T] {
  type Rec
  type Ref

  def readFrom(rec: Rec, ref: Ref): T
}

trait StorageType[T]
extends ReadFromStorage[T] {

  implicit def `implicit`: this.type = this
  def jvmType: Class[T]
  def writeAs(value: T): AnyRef = value.asInstanceOf[AnyRef]

}
