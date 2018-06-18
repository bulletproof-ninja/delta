package delta.ddd

import java.util.concurrent.{ CountDownLatch, LinkedBlockingQueue, TimeUnit }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import org.junit.Assert._
import org.junit.Test
import delta.util.ConcurrentMapRepository
import delta.util.PublishingRepository

class TestRepository {

  private def withLatch(count: Int)(thunk: CountDownLatch => Unit): Unit = {
    val latch = new CountDownLatch(count)
    thunk(latch)
    assertTrue(latch.await(5, TimeUnit.SECONDS))
  }
  import language.implicitConversions
  implicit def toFut[T](t: T) = Future successful t
  case class Customer(name: String, postCode: String)

  @Test
  def `insert success`(): Unit = {
    withLatch(5) { latch =>
      val repo = new ConcurrentMapRepository[BigInt, Customer]
      val hank = Customer("Hank", "12345")
      repo.insert(5, hank).foreach { id5 =>
        assertEquals(BigInt(5), id5)
        latch.countDown()
        val update1 = repo.update(5, Revision(0)) {
          case (customer, rev) =>
            assertEquals(0, rev)
            latch.countDown()
            customer
        }
        update1.foreach { rev =>
          assertEquals(1, rev)
          latch.countDown()
          val update2 = repo.update(5, Map("hello"->"world")) {
            case (customer, rev) =>
              assertEquals(1, rev)
              latch.countDown()
              customer.copy(name = customer.name.toUpperCase)
          }
          update2.foreach { rev =>
            assertEquals(2, rev)
            latch.countDown()
          }
        }
      }
    }
  }

  sealed trait VeryBasicEvent
  case object CustomerUpdated extends VeryBasicEvent
  case object CustomerCreated extends VeryBasicEvent

  @Test
  def `event publishing`(): Unit = {
    case class Notification(id: Long, revision: Int, events: List[VeryBasicEvent], metadata: Map[String, String])
    val notifications = new LinkedBlockingQueue[Notification]
    val repo = new PublishingRepository[Long, Customer, VeryBasicEvent](new ConcurrentMapRepository, global) {
      type Event = VeryBasicEvent
      def publish(id: Long, revision: Int, events: List[Event], metadata: Map[String, String]): Unit = {
        notifications offer Notification(id, revision, events, metadata)
      }
    }
    val hank = Customer("Hank", "12345")
    repo.insert(5L, hank -> List(CustomerCreated))
    val n1 = notifications.take
    assertEquals(5L, n1.id)
    assertEquals(0, n1.revision)
    assertEquals(1, n1.events.size)
    assertEquals(CustomerCreated, n1.events.head)
    repo.update[Nothing](5, Revision(0)) {
      case ((hank, _), _) =>
        hank.copy(name = "Hankster") -> List(CustomerUpdated)
    }
    val n2 = notifications.take
    assertEquals(5, n2.id)
    assertEquals(1, n2.revision)
    assertEquals(1, n2.events.size)
    assertEquals(CustomerUpdated, n2.events.head)

    withLatch(1) { latch =>
      repo.load(5).foreach {
        case ((hank, _), rev) =>
          assertEquals(1, rev)
          assertEquals("Hankster", hank.name)
          latch.countDown()
      }
    }
  }

}
