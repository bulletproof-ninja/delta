package ulysses.ddd

import scuff.ddd._

/**
 * Trait that enables snapshotting at particular
 * revision intervals. Does not implement snapshot
 * storage.
 */
trait FixedIntervalSnapshots[S <: AnyRef, ID] extends SnapshotStore[S, ID] {

  /**
   * Interval between revisions.
   */
  protected def revisionInterval: Int

  override def assumeSnapshotCurrent = revisionInterval == 1

  abstract override def save(id: ID, snapshot: Snapshot) {
    if ((snapshot.revision + 1) % revisionInterval == 0) {
      super.save(id, snapshot)
    }
  }

}
