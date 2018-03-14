package delta

import scala.concurrent.duration.DurationInt

import scuff.{ LamportClock, Memoizer }
import scuff.concurrent.ScuffScalaFuture

final class LamportTicker private (impl: LamportClock, onClose: () => Unit) extends Ticker {
  def nextTick(): Long = impl.next()
  def nextTick(lastTick: Long): Long = impl.next(lastTick)
  def close = onClose()
}

object LamportTicker {

  private[this] val subscribingClocks = {
      def factory(es: EventSource[_, _, _]): LamportTicker = {
        val maxTick = es.maxTick.await(111.seconds) getOrElse -1L
        val clock = new LamportClock(maxTick)
        val subscription = es.subscribe() { txn =>
          clock.sync(txn.tick)
        }
        new LamportTicker(clock, subscription.cancel _)
      }
    new Memoizer[EventSource[_, _, _], LamportTicker](factory)
  }

  /**
    * Internally managed Lamport ticker.
    */
  def apply(es: EventSource[_, _, _]): LamportTicker = subscribingClocks(es)

  /** Use a custom {{scuff.LamportClock}} implementation. */
  def apply(impl: LamportClock, shutdown: () => Unit = () => ()) = {
    new LamportTicker(impl, shutdown)
  }

}
