package ulysses

import java.io.{ ObjectInputStream, ObjectOutputStream }
import scala.language.implicitConversions
import org.junit.{ Before, Test }
import org.junit.Assert._
import scuff.concurrent.Threads
import scuff.io.{ ByteInputStream, ByteOutputStream }
import scuff.reflect.Surgeon
import scuff.JavaSerializer
import scala.concurrent.ExecutionContext
import ulysses.util.LocalPublishing

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

    def name(cls: EventClass): String = cls.getName

    def encode(evt: Event): Array[Byte] =
      JavaSerializer.encode(evt)
    def decode(name: String, data: Array[Byte]): Event =
      JavaSerializer.decode(data).asInstanceOf[Event]
  }

  private[this] val es = new util.TransientEventStore[Symbol, Event, String, Array[Byte]](
      RandomDelayExecutionContext) with LocalPublishing[Symbol, Event, String] {
    def publishCtx = RandomDelayExecutionContext
  }

  @Test
  def serialization {
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
