package delta.testing

import java.io.{ ObjectInputStream, ObjectOutputStream }
import scuff.io.{ ByteInputStream, ByteOutputStream }
import scuff.JavaSerializer
import delta._

class TestEventStore
extends BaseTest {

  final val channel = Channel("USER")

  object EvtFmt
      extends EventFormat[Event, Array[Byte]] {

    def getVersion(cls: EventClass) = NotVersioned
    def getName(cls: EventClass): String = cls.getName

    def encode(evt: Event): Array[Byte] =
      JavaSerializer.encode(evt)
    def decode(encoded: Encoded): Event =
      JavaSerializer.decode(encoded.data).asInstanceOf[Event]
  }

  private[this] val es =
    new util.TransientEventStore[Symbol, Event, Array[Byte]](
        ec, EvtFmt) {
      def ticker = SysClockTicker
    }

  test("serialization") {
    val id12 = Symbol("id12")
    val wallClock = System.currentTimeMillis
    val tx = new es.Transaction(99, channel, id12, 42, Map("wallClock" -> wallClock.toString), List(Event.AgeChanged(100), Event.NameChanged("Hansi")))
    val out = new ByteOutputStream
    val objOut = new ObjectOutputStream(out)
    objOut.writeObject(tx)
    objOut.close()
    val bytes = out.toArray
    val objInp = new ObjectInputStream(new ByteInputStream(bytes))
    val outTx = objInp.readObject().asInstanceOf[es.Transaction]
    assert(0 === objInp.available)
    assert(tx === outTx)
  }
}
