package ulysses.ddd

import scala.concurrent.duration.DurationInt
import scuff.{LamportClock, Multiton}
import scuff.concurrent.ScuffScalaFuture
import ulysses.Publishing
import ulysses.TimeOrdering

class LamportTimestamps private (clock: LamportClock) extends Timestamps {
  def nextTimestamp(greaterThan: Long): Long = clock.next(greaterThan)
}

object LamportTimestamps {

  type ES = Publishing[_, _, _] with TimeOrdering[_, _, _]

  private def factory(es: ES): LamportTimestamps = {
    val lastTimestamp = es.latestTimestamp.get(10.seconds).getOrElse(-1L)
    val clock = new LamportClock(lastTimestamp)
    es.subscribe(txn => clock.sync(txn.clock), _ => true)
    new LamportTimestamps(clock)
  }

  private[this] val onePerEventStore = new Multiton(factory)

  /**
   * Internally managed Lamport clocks.
   */
  def apply(es: ES) = onePerEventStore(es)

  /**
   * Externally managed Lamport clocks.
   */
  def apply(clock: LamportClock) = new LamportTimestamps(clock)

}
