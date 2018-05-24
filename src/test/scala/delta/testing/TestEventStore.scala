package delta.testing

import java.io.{ ObjectInputStream, ObjectOutputStream }
import org.junit.Test
import org.junit.Assert._
import scuff.io.{ ByteInputStream, ByteOutputStream }
import scuff.JavaSerializer
import delta._
import delta.NoVersioning
import delta.util.LocalPublisher

sealed trait Event
object Event {
  case class NameChanged(newName: String) extends Event
  case class AgeChanged(newAge: Int) extends Event
}
case class User(name: String, age: Int)

class TestEventStore {

  implicit object EvtCodec
      extends EventCodec[Event, Array[Byte]]
      with NoVersioning[Event, Array[Byte]] {

    def getName(cls: EventClass): String = cls.getName

    def encode(evt: Event): Array[Byte] =
      JavaSerializer.encode(evt)
    def decode(channel: String, name: String, data: Array[Byte]): Event =
      JavaSerializer.decode(data).asInstanceOf[Event]
  }

  private[this] val es = new util.TransientEventStore[Symbol, Event, Array[Byte]](
      RandomDelayExecutionContext) with Publishing[Symbol, Event] {
      val publisher = new LocalPublisher[Symbol, Event](RandomDelayExecutionContext)
  }

  @Test
  def serialization() {
    val wallClock = System.currentTimeMillis
    val txn = new es.TXN(99, "USER", 'id12, 42, Map("wallClock" -> wallClock.toString), List(Event.AgeChanged(100), Event.NameChanged("Hansi")))
    val out = new ByteOutputStream
    val objOut = new ObjectOutputStream(out)
    objOut.writeObject(txn)
    objOut.close()
    val bytes = out.toArray
    val objInp = new ObjectInputStream(new ByteInputStream(bytes))
    val outTxn = objInp.readObject().asInstanceOf[es.TXN]
    assertEquals(0, objInp.available)
    assertEquals(txn, outTxn)
  }
}
