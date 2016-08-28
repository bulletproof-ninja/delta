package ulysses.ddd

object SystemClock extends Clock {
  def nextTick(lastTick: Long): Long = (lastTick + 1) max System.currentTimeMillis
  def close = ()
}
