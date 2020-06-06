package delta

object SysClockTicker extends Ticker {
  def nextTick(): Long = System.currentTimeMillis
  def nextTick(lastTick: Tick): Long = (lastTick + 1) max nextTick()
  def close = ()
}
