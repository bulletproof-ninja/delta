import delta.EventCodec
import delta.NoVersioning
import scuff.JavaSerializer

package object foo {
  implicit object BinaryEventCodec
    extends EventCodec[MyEvent, Array[Byte]]
    with NoVersioning[MyEvent, Array[Byte]] {

    protected def getName(cls: EventClass): String =  cls.getName
    def encode(evt: MyEvent): Array[Byte] = JavaSerializer.encode(evt)
    def decode(channel: String, name: String,data: Array[Byte]): MyEvent =
      JavaSerializer.decode(data).asInstanceOf[MyEvent]

  }
}
