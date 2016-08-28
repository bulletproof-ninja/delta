package ulysses.ddd.cqrs

import ulysses.Transaction

trait EventStreamTracker[ID] {
  /** Last tick seen. */
  def lastTick: Option[Long]
  /** Expected stream revision. */
  def expectedRevision(id: ID): Int
  /** Mark id/revision as consumed. */
  def markAsConsumed(txn: Transaction[ID, _, _]): Unit
  /** Callback when stream goes live. */
  def onGoingLive(): Unit

}
