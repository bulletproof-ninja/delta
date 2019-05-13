package delta

import scala.concurrent.duration.DurationInt

import scuff.{ LamportClock, Memoizer }
import scuff.concurrent.ScuffScalaFuture
import scala.util.Try
import scala.util.control.NonFatal
import scala.util.Failure

final class LamportTicker private (impl: LamportClock, onClose: () => Unit) extends Ticker {
  @volatile private[this] var closed = false
  @inline private def checkClosed(): Unit =
    if (closed) throw new IllegalStateException(s"Ticker has been closed.")

  def nextTick(): Long = {
    checkClosed()
    impl.next()
  }

  def nextTick(lastTick: Long): Long = {
    checkClosed()
    impl.next(lastTick)
  }

  def close = {
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
          es.subscribe()(txn => clock.sync(txn.tick))
        }
        subscription.map(sub => new LamportTicker(clock, sub.cancel _))

      }
    new Memoizer[EventSource[_, _], Try[LamportTicker]](factory)
  }

  /**
    * Internally managed Lamport ticker.
    * @param es The event source, which must support publishing.
    * @return The Lamport ticker, or `None` if event source does not support publishing
    */
  def apply(es: EventSource[_, _]): LamportTicker = subscribingClocks(es).recoverWith {
    case NonFatal(cause) => Failure {
      val lamportTicker = classOf[LamportTicker].getName
      new IllegalArgumentException(s"Cannot create $lamportTicker from event source: $es", cause)
    }
  }.get

  /** Use a custom {{scuff.LamportClock}} implementation. */
  def apply(impl: LamportClock, shutdown: () => Unit = () => ()) = {
    new LamportTicker(impl, shutdown)
  }

}
