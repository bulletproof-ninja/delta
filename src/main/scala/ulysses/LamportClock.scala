package ulysses

import scala.concurrent.duration.DurationInt
import scala.language.existentials

import scuff.Memoizer
import scuff.concurrent.ScuffScalaFuture

final class LamportClock private (impl: scuff.LamportClock, onClose: () => Unit) extends Clock {
  def nextTick(lastTick: Long): Long = impl.next(lastTick)
  def close = onClose()
}

object LamportClock {

  private[this] val subscribingClocks = {
      def factory(es: EventSource[_, _, _]): LamportClock = {
        val lastTick = es.lastTick.await(111.seconds) getOrElse -1L
        val clock = new scuff.LamportClock(lastTick)
        val subscription = es.subscribe() { txn =>
          clock.sync(txn.tick)
        }
        new LamportClock(clock, subscription.cancel)
      }
    new Memoizer[EventSource[_, _, _], LamportClock](factory)
  }

  /**
    * Internally managed Lamport clocks.
    */
  def apply(es: EventSource[_, _, _]): LamportClock = subscribingClocks(es)

  /** Use a custom {{scuff.LamportClock}} implementation. */
  def apply(impl: scuff.LamportClock, shutdown: () => Unit = () => ()) = {
    new LamportClock(impl, shutdown)
  }

}
