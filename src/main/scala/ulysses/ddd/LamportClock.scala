package ulysses.ddd

import scala.concurrent.duration.DurationInt
import scuff.Memoizer
import scuff.concurrent.ScuffScalaFuture
import ulysses.Publishing
import scuff.concurrent.StreamCallback
import scuff.Subscription

class LamportClock private (impl: scuff.LamportClock, subscription: Subscription) extends Clock {
  def nextTick(lastTick: Long): Long = impl.next(lastTick)
  def close = subscription.cancel()
}

object LamportClock {

  type ES = Publishing[_, _, _]

  private def factory(es: ES): LamportClock = {
    val lastTick = es.tickRange.get(20.seconds).map(_._2).getOrElse(-1L)
    val clock = new scuff.LamportClock(lastTick)
    val clockSyncher = StreamCallback { txn: ES#TXN => clock.sync(txn.tick) }
    val subscription: Subscription = ???
//    FIXME: = es.subscribe()(clockSyncher)
    new LamportClock(clock, subscription)
  }

  private[this] val onePerEventStore = new Memoizer(factory)

  /**
    * Internally managed Lamport clocks.
    */
  def apply(es: ES): LamportClock = onePerEventStore(es)

  /** Use a custom {{scuff.LamportClock}} implementation. */
  def apply(impl: scuff.LamportClock, shutdown: () => Unit = () => ()) = {
    val subscription = new Subscription {
      def cancel() = shutdown()
    }
    new LamportClock(impl, subscription)
  }

}
