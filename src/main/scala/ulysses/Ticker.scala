package ulysses

trait Ticker extends AutoCloseable {
  /**
    * Produce a new tick.
    */
  def nextTick(): Long
  /**
    * Produce a new tick, which is
    * causally dependent on the
    * provided tick.
    * @param lastTick Causally dependent tick
    */
  def nextTick(lastTick: Long): Long
}
