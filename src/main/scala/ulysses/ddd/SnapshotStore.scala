package ulysses.ddd

import scala.concurrent.Future

/**
  * Snapshot storage implementation.
  */
trait SnapshotStore[S, ID] {
  type Snapshot = SnapshotStore.Snapshot[S]

  /** Load snapshot, if exists. */
  def load(id: ID): Future[Option[Snapshot]]
  /**
    *  Save snapshot.
    *  This method should not throw an exception,
    *  but handle/report it internally.
    */
  def save(id: ID, snapshot: Snapshot): Unit

  /**
    * Should a snapshot be considered current? Defaults to `false`.
    * Basically this should only be overridden if all snapshots
    * are saved (in non-local location if distributed environment).
    * Assuming snapshots are current will prevent a query to event store.
    * If not true (due to race conditions or snapshots not always stored),
    * the event store will still be queried after detecting non-currency.
    * In other words, having this be `true` will still work, but will be
    * slightly less efficient if not actually true most of the time.
    */
  def assumeSnapshotCurrent = false

}

object SnapshotStore {
  /**
    * @param state Snapshot state
    * @param revision Snapshot revision
    * @param tick Transaction clock
    */
  case class Snapshot[S](state: S, revision: Int, tick: Long)

  private[this] val Empty = new SnapshotStore[AnyRef, Any] {
    private[this] val NoSnapshot = Future.successful(None)
    def load(id: Any): Future[Option[Snapshot]] = NoSnapshot
    def save(id: Any, snapshot: Snapshot): Unit = ()
  }

  def NoSnapshots[S, ID]: SnapshotStore[S, ID] = Empty.asInstanceOf[SnapshotStore[S, ID]]
}
