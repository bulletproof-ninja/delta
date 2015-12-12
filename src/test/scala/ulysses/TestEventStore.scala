package ulysses

import java.io.{ ObjectInputStream, ObjectOutputStream }
import scala.language.implicitConversions
import org.junit.{ Before, Test }
import org.junit.Assert._
import scuff.concurrent.Threads
import scuff.io.{ ByteInputStream, ByteOutputStream }
import scuff.reflect.Surgeon

sealed trait Event
object Event {
  case class NameChanged(newName: String) extends Event
  case class AgeChanged(newAge: Int) extends Event
}
case class User(name: String, age: Int)

class TestEventStore {

  private[this] val es: EventStore[Symbol, Event, String] = null

  @Before
  def setup {
    new Surgeon(this).set('es, new util.TransientEventStore[Symbol, Event, String](Threads.PiggyBack))
  }

  @Test
  def serialization {
    val wallClock = System.currentTimeMillis
    val txn = new es.Transaction(99, "USER", 'id12, 42, Map("wallClock" -> wallClock.toString), List(Event.AgeChanged(100), Event.NameChanged("Hansi")))
    val out = new ByteOutputStream
    val objOut = new ObjectOutputStream(out)
    objOut.writeObject(txn)
    objOut.close()
    val bytes = out.toArray
    val objInp = new ObjectInputStream(new ByteInputStream(bytes))
    val outTxn = objInp.readObject().asInstanceOf[es.Transaction]
    assertEquals(0, objInp.available)
    assertEquals(txn, outTxn)
  }
}
