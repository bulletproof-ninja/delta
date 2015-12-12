package ulysses.ddd.cqrs

trait EventStreamTracker[ID] {
  /** Last clock seen. */
  def lastTimestamp: Option[Long]
  /** Expected stream revision. */
  def expectedRevision(id: ID): Int
  /** Mark id/revision as consumed. */
  def markAsConsumed(id: ID, rev: Int, clock: Long): Unit
  /** Callback when stream goes live. */
  def onGoingLive(): Unit

}
