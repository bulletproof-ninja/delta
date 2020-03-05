package delta.testing

import java.io.{ ObjectInputStream, ObjectOutputStream }
import org.junit.Test
import org.junit.Assert._
import scuff.io.{ ByteInputStream, ByteOutputStream }
import scuff.JavaSerializer
import delta._
import delta.util.LocalHub
import scuff.Codec

sealed trait Event
object Event {
  case class NameChanged(newName: String) extends Event
  case class AgeChanged(newAge: Int) extends Event
}
case class User(name: String, age: Int)

class TestEventStore {

  final val Channel = Transaction.Channel("USER")

  object EvtFmt
      extends EventFormat[Event, Array[Byte]] {

    def getVersion(cls: EventClass) = NoVersion
    def getName(cls: EventClass): String = cls.getName

    def encode(evt: Event): Array[Byte] =
      JavaSerializer.encode(evt)
    def decode(encoded: Encoded): Event =
      JavaSerializer.decode(encoded.data).asInstanceOf[Event]
  }

  private[this] val es = new util.TransientEventStore[Symbol, Event, Array[Byte]](
      RandomDelayExecutionContext, EvtFmt)(_ => SysClockTicker)
      with MessageHubPublishing[Symbol, Event] {
      def toTopic(ch: Channel) = Topic(s"transactions/$ch")
      def toTopic(tx: Transaction): Topic = toTopic(tx.channel)
      val txHub = new LocalHub[Transaction](toTopic, RandomDelayExecutionContext)
      val txChannels = Set(Channel)
      val txCodec = Codec.noop[Transaction]
  }

  @Test
  def serialization(): Unit = {
    val id12 = Symbol("id12")
    val wallClock = System.currentTimeMillis
    val tx = new es.Transaction(99, Channel, id12, 42, Map("wallClock" -> wallClock.toString), List(Event.AgeChanged(100), Event.NameChanged("Hansi")))
    val out = new ByteOutputStream
    val objOut = new ObjectOutputStream(out)
    objOut.writeObject(tx)
    objOut.close()
    val bytes = out.toArray
    val objInp = new ObjectInputStream(new ByteInputStream(bytes))
    val outTx = objInp.readObject().asInstanceOf[es.Transaction]
    assertEquals(0, objInp.available)
    assertEquals(tx, outTx)
  }
}
