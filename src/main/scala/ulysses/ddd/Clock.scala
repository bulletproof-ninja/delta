package ulysses.ddd

trait Clock extends AutoCloseable {
  /**
    * Produce a new tick, which must always
    * be greater than the provided tick (if any),
    * to ensure causal ordering.
    */
  def nextTick(lastTick: Long = Long.MinValue): Long
}

object Clock {
  def System: Clock = SystemClock
}
