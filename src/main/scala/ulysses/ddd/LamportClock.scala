package ulysses.ddd

import scala.concurrent.duration.DurationInt
import scuff.Memoizer
import scuff.concurrent.ScuffScalaFuture
import ulysses.Publishing
import scuff.concurrent.{ StreamCallback, StreamPromise }
import scuff.Subscription
import language.existentials
import ulysses.StreamFilter

class LamportClock private (impl: scuff.LamportClock, onClose: () => Unit) extends Clock {
  def nextTick(lastTick: Long): Long = impl.next(lastTick)
  def close = onClose()
}

object LamportClock {

  private def factory[ID, EVT, CH](es: Publishing[ID, EVT, CH]): LamportClock = {
    val lastTick = es.lastTick.get(111.seconds) getOrElse -1L
    val clock = new scuff.LamportClock(lastTick)
    val clockSyncher = StreamPromise[es.TXN] { txn =>
      clock.sync(txn.tick)
    }
    val subscription = es.subscribe()(clockSyncher)
    new LamportClock(clock, subscription.cancel)
  }

  /**
    * Internally managed Lamport clocks.
    */
  def apply[ID, EVT, CH](es: Publishing[ID, EVT, CH]): LamportClock = factory(es)

  /** Use a custom {{scuff.LamportClock}} implementation. */
  def apply(impl: scuff.LamportClock, shutdown: () => Unit = () => ()) = {
    new LamportClock(impl, shutdown)
  }

}
