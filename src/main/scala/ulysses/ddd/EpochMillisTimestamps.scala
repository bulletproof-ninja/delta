package ulysses.ddd

object EpochMillisTimestamps extends Timestamps {

  def nextTimestamp(greaterThan: Long): Long = (greaterThan + 1) max System.currentTimeMillis

}
