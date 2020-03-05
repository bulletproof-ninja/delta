package delta.conv

trait StorageType[T] {
  implicit def `implicit`: this.type = this

  type Rec
  type Ref

  def jvmType: Class[T]
  def writeAs(value: T): AnyRef = value.asInstanceOf[AnyRef]
  def readFrom(rec: Rec, ref: Ref): T

}
