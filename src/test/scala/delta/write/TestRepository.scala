package delta.write

import delta._
import delta.util.ConcurrentMapRepository
import delta.util.PublishingRepository
import delta.testing._

import java.util.concurrent.{ CountDownLatch, LinkedBlockingQueue, TimeUnit }

import org.junit.Assert._
import org.junit.Test

class TestRepository {

  private def withLatch(count: Int)(thunk: CountDownLatch => Unit): Unit = {
    val latch = new CountDownLatch(count)
    thunk(latch)
    assertTrue(latch.await(5, TimeUnit.SECONDS))
  }

  case class Customer(name: String, postCode: String)

  implicit def ec = RandomDelayExecutionContext
  implicit def metadata = Metadata("hello" -> "world")

  @Test
  def `insert success`(): Unit = {
    withLatch(5) { latch =>
      val repo = new ConcurrentMapRepository[BigInt, Customer]
      val hank = Customer("Hank", "12345")
      repo.insert(5, hank).foreach { id5 =>
        assertEquals(BigInt(5), id5)
        latch.countDown()
        val update1 = repo.update(5, 0) {
          case (customer, rev) =>
            assertEquals(0, rev)
            latch.countDown()
            customer
        }
        update1.foreach { rev =>
          assertEquals(1, rev)
          latch.countDown()
          val update2 = repo.update(5) {
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
    case class Notification(id: Long, revision: Revision, events: List[VeryBasicEvent], metadata: Metadata)
    val notifications = new LinkedBlockingQueue[Notification]
    val repo = new PublishingRepository[Long, Customer, VeryBasicEvent](
        new ConcurrentMapRepository, ec) {
      def publish(id: Long, revision: Revision, events: List[VeryBasicEvent], metadata: Metadata): Unit = {
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
    repo.update[Nothing](5, Some(0)) {
      case (hank, _) =>
        hank.copy(name = "Hankster") -> List(CustomerUpdated)
    }
    val n2 = notifications.take
    assertEquals(5, n2.id)
    assertEquals(1, n2.revision)
    assertEquals(1, n2.events.size)
    assertEquals(CustomerUpdated, n2.events.head)

    withLatch(1) { latch =>
      repo.load(5).foreach {
        case (hank, rev) =>
          assertEquals(1, rev)
          assertEquals("Hankster", hank.name)
          latch.countDown()
      }
    }
  }

}
