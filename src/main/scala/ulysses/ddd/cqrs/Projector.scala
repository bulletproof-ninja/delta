//package ulysses.ddd.cqrs
//
//import scuff.Subscription
//import java.util.concurrent.CountDownLatch
//import java.util.concurrent.TimeUnit
//import scuff.Feed
//import scala.util.Try
//import concurrent.duration._
//import scala.concurrent.Future
//import scala.util.Failure
//import scala.util.Success
//import scuff.concurrent.{ StreamCallback, StreamResult }
//import scala.util.control.NonFatal
//import scuff.concurrent.Threads.PiggyBack
//
//trait Projector {
//
//  /** Public publish format. */
//  type PUB
//  /** Internal data format. */
//  protected type DAT <: Data
//  /** Internal filter. */
//  protected type F <: Filter
//
//  protected trait Data {
//    def toPublish(to: F): PUB
//  }
//
//  protected trait Filter extends (DAT => Boolean) {
//    @inline
//    final def apply(data: DAT): Boolean = matches(data)
//    def matches(data: DAT): Boolean
//  }
//
//  protected val store: DataStore
//
//  protected def query(filter: F)(callback: StreamCallback[DAT])
//
//  protected def feed: Feed {
//    type Consumer = (DAT => Unit)
//    type Filter = DAT
//  }
//
//  /**
//   * @param filter The subscription/query filter
//   * @param subscriber The callback function
//   * @param strict If `true`, eliminates the, perhaps remote, possibility of out-of-order revisions
//   */
//  protected final def subscribe(filter: F, subscriber: PUB => Unit, strict: Boolean = true): Future[Subscription] = {
//    val latch = newLatch(strict)
//      def proxySubscriber(data: DAT) {
//        latch.await()
//        subscriber(data.toPublish(filter))
//      }
//    subscribeToFeed(filter, subscriber, proxySubscriber, latch)
//  }
//
//  private def subscribeToFeed(filter: F, realSubscriber: PUB => Unit, proxySubscriber: DAT => Unit, latch: Latch): Future[Subscription] = {
//    try {
//      val subscription = feed.subscribe(filter)(proxySubscriber)
//      val callback = StreamResult(subscription) { data: DAT =>
//        val msg = data.toPublish(filter)
//        realSubscriber(msg)
//      }
//      query(filter)(callback)
//      callback.future.andThen { case _ => latch.open() }(PiggyBack)
//      callback.future
//    } catch {
//      case NonFatal(e) =>
//        latch.open()
//        Future failed e
//    }
//  }
//
//  private trait Latch {
//    @inline
//    def await()
//    def open()
//  }
//
//  /** When using `strict` subscribe, max time to wait for the initial data lookup. */
//  protected val maxDataStoreLookupWaitOnStrict: FiniteDuration = 5.seconds
//
//  private def newLatch(strict: Boolean): Latch = {
//    if (strict) {
//      new Latch {
//        val cdl = new CountDownLatch(1)
//        @inline
//        def await = cdl.await(maxDataStoreLookupWaitOnStrict.length, maxDataStoreLookupWaitOnStrict.unit)
//        def open = cdl.countDown()
//      }
//    } else {
//      new Latch {
//        @inline
//        def await = ()
//        def open = ()
//      }
//    }
//  }
//
//}
