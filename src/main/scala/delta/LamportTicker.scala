package delta

import scala.concurrent.duration.DurationInt

import scuff.{ LamportClock, Memoizer }
import scuff.concurrent.ScuffScalaFuture
import scala.util.Try
import scala.util.control.NonFatal
import scala.util.Failure

final class LamportTicker private (impl: LamportClock, onClose: () => Unit)
extends Ticker {

  @volatile private[this] var closed = false
  @inline private def checkClosed(): Unit =
    if (closed) throw new IllegalStateException(s"${getClass.getName} has been closed.")

  def nextTick(): Tick = {
    checkClosed()
    impl.next()
  }

  def nextTick(lastTick: Tick): Tick = {
    checkClosed()
    impl next lastTick
  }

  def close = if (!closed) {
    closed = true
    Try(onClose)
  }
}

object LamportTicker {

  private[this] val subscribingClocks = {
      def factory(es: EventSource[_, _]): Try[LamportTicker] = {
        val maxTick = es.maxTick.await(111.seconds) getOrElse -1L
        val clock = new LamportClock(maxTick)
        val subscription = Try {
          es.subscribeGlobal()(tx => clock.sync(tx.tick))
        }
        subscription.map(sub => new LamportTicker(clock, sub.cancel _))

      }
    new Memoizer[EventSource[_, _], Try[LamportTicker]](factory)
  }

  /**
    * Internally managed Lamport ticker that subscribes to transactions
    * from event source.
    * @note This method only makes sense in a distributed environment, where
    * the event source transactions are broadcast to all members.
    * @param es The event source, which must support publishing.
    * @return The Lamport ticker
    */
  @throws[IllegalArgumentException]
  def subscribeTo(es: EventSource[_, _]): LamportTicker = subscribingClocks(es).recoverWith {
    case NonFatal(cause) => Failure {
      new IllegalArgumentException(s"Cannot subscribe to event source: $es", cause)
    }
  }.get

  /** Use a custom {{scuff.LamportClock}} implementation. */
  def apply(impl: LamportClock, shutdown: () => Unit = () => () ) = {
    new LamportTicker(impl, shutdown)
  }

}
