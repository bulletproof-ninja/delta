package delta.util

import java.util.concurrent.{ CountDownLatch, TimeoutException }
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.DurationInt

import scuff.Subscription
import scuff.concurrent.{ StreamCallback, StreamPromise }
import delta.{ EventSource, Transaction }

trait DurableConsumer[ID, EVT, CH] {

  type TXN = Transaction[ID, EVT, CH]
  type ES = EventSource[ID, EVT, CH]

  protected def liveSwitchProcessingTimeout = 33.seconds
  protected def selector[T <: ES](es: T): T#Selector
  protected def newProcessor(es: ES): TwoPhaseProcessor[TXN]

  /**
    * Resume consumption.
    * @param es The [[EventSource]] to process.
    * @param maxTickSkew The anticipated max tick skew.
    */
  def resume(es: ES, maxTickSkew: Int)(implicit ec: ExecutionContext): Future[Subscription] = {
    require(maxTickSkew >= 0, s"Cannot have negative tick skew: $maxTickSkew")
    es.lastTick.flatMap { lastTickAtStart =>
      val processor = newProcessor(es)
      val catchUpQuery = processor.lastProcessedTick match {
        case None =>
          es.query(selector(es)) _
        case Some(lastSeen) =>
          es.querySince(lastSeen - maxTickSkew, selector(es)) _
      }
      val catchUpFuture = StreamPromise.foreach(catchUpQuery)(processor.historic)
      catchUpFuture.map { _ =>
        val switcher = {
          new SafeCatchUpSwitcher(processor.live)
        }
        val liveSubscription = es.subscribe(selector(es))(switcher.live)
        lastTickAtStart match {
          case None =>
            es.query(selector(es))(switcher.callback(ec.reportFailure))
          case Some(lastTickAtStart) =>
            es.querySince(lastTickAtStart - maxTickSkew, selector(es))(switcher.callback(ec.reportFailure))
        }

        liveSubscription
      }
    }
  }

  private final class SafeCatchUpSwitcher(process: TXN => Unit) {
    private[this] val state = new AtomicReference[Either[List[TXN], CountDownLatch]](Left(Nil))
    def live(txn: TXN): Unit = state.get match {
      case null => process(txn)
      case ll @ Left(list) =>
        if (!state.compareAndSet(ll, Left(txn :: list))) {
          live(txn) // Retry
        }
      case Right(latch) =>
        val latchTimeout = liveSwitchProcessingTimeout
        if (latch.await(latchTimeout.length, latchTimeout.unit)) {
          state.set(null)
          process(txn)
        } else {
          throw new TimeoutException(s"Timed out after $latchTimeout, awaiting catch-up processing")
        }
    }
    def callback(reportFailure: Throwable => Unit) = new StreamCallback[TXN] {
      def onNext(txn: TXN): Unit = process(txn)
      def onError(th: Throwable): Unit = reportFailure(th)
      def onCompleted(): Unit = {
        val ll = state.get
        val list = ll.left.get
        val latch = new CountDownLatch(1)
        if (state.compareAndSet(ll, Right(latch))) {
          list.reverseIterator.foreach(process)
          latch.countDown()
        } else onCompleted() // Retry
      }

    }
  }

}
