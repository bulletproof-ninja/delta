package delta

trait Ticker extends AutoCloseable {
  /**
    * Produce a new tick.
    */
  def nextTick(): Tick
  /**
    * Produce a new tick, which is
    * strictly greater than the
    * provided tick.
    * @param lastTick Causally dependent tick
    */
  def nextTick(lastTick: Tick): Tick

}
