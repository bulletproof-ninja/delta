package ulysses.ddd

trait Clock extends AutoCloseable {
  /**
    * Produce a new tick. The tick will always
    * be greater than the provided tick (if any),
    * to ensure ordering of causal events.
    */
  def nextTick(lastTick: Long = Long.MinValue): Long
}
