import delta.EventFormat
import scuff.JavaSerializer

package object foo {
  object BinaryEventFormat
    extends EventFormat[MyEvent, Array[Byte]] {

    protected def getVersion(cls: EventClass) = NotVersioned
    protected def getName(cls: EventClass): String =  cls.getName

    def encode(evt: MyEvent): Array[Byte] = JavaSerializer.encode(evt)
    def decode(encoded: Encoded): MyEvent =
      JavaSerializer.decode(encoded.data).asInstanceOf[MyEvent]

  }
}
