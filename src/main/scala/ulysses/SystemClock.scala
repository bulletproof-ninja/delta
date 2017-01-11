package ulysses

object SysClockTicker extends Ticker {
  def nextTick(): Long = System.currentTimeMillis
  def nextTick(lastTick: Long): Long = (lastTick + 1) max nextTick()
  def close = ()
}
