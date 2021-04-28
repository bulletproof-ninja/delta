package delta.write

import delta._
import delta.util.{ ConcurrentMapRepository, PublishingRepository }
import delta.testing._

import java.util.concurrent.{ CountDownLatch, LinkedBlockingQueue, TimeUnit }

class TestRepository
extends BaseTest {

  private def withLatch(count: Int)(thunk: CountDownLatch => Unit): Unit = {
    val latch = new CountDownLatch(count)
    thunk(latch)
    assert(latch.await(5, TimeUnit.SECONDS))
  }

  case class Customer(name: String, postCode: String)

  implicit def metadata = Metadata("hello" -> "world")

  test("insert success") {
    withLatch(5) { latch =>
      val repo = new ConcurrentMapRepository[BigInt, Customer]
      val hank = Customer("Hank", "12345")
      repo.insert(5, hank).foreach { id5 =>
        assert(BigInt(5) === id5)
        latch.countDown()
        val update1 = repo.update(5, 0) {
          case (customer, rev) =>
            assert(0 === rev)
            latch.countDown()
            customer
        }
        update1.foreach { rev =>
          assert(1 === rev)
          latch.countDown()
          val update2 = repo.update(5) {
            case (customer, rev) =>
              assert(1 === rev)
              latch.countDown()
              customer.copy(name = customer.name.toUpperCase)
          }
          update2.foreach { rev =>
            assert(2 === rev)
            latch.countDown()
          }
        }
      }
    }
  }

  sealed trait VeryBasicEvent
  case object CustomerUpdated extends VeryBasicEvent
  case object CustomerCreated extends VeryBasicEvent

  test("event publishing") {
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
    assert(5L === n1.id)
    assert(0 === n1.revision)
    assert(1 === n1.events.size)
    assert(CustomerCreated === n1.events.head)
    repo.update(5, Some(0)) {
      case (hank, _) =>
        hank.copy(name = "Hankster") -> List(CustomerUpdated)
    }
    val n2 = notifications.take
    assert(5 === n2.id)
    assert(1 === n2.revision)
    assert(1 === n2.events.size)
    assert(CustomerUpdated === n2.events.head)

    withLatch(1) { latch =>
      repo.load(5).foreach {
        case (hank, rev) =>
          assert(1 === rev)
          assert("Hankster" === hank.name)
          latch.countDown()
      }
    }
  }

}
