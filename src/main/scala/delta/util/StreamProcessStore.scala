package delta.util

import collection.Map
import delta.SnapshotStore
import scala.concurrent.Future
import scala.concurrent.ExecutionContext

trait StreamProcessStore[K, S] extends SnapshotStore[K, S] {

  def readBatch(keys: Iterable[K]): Future[Map[K, Snapshot]]
  def writeBatch(batch: Map[K, Snapshot]): Future[Unit]
  def refresh(key: K, revision: Int, tick: Long): Future[Unit]
  def refreshBatch(revisions: Map[K, (Int, Long)]): Future[Unit]

  /**
    *  Write snapshot, if absent.
    *  Otherwise return present snapshot.
    *  @return `None` if write was successful, or `Some` present snapshot
    */
  protected def writeIfAbsent(key: K, snapshot: Snapshot): Future[Option[Snapshot]]
  /**
    *  Write replacement snapshot, if old snapshot matches.
    *  Otherwise return current snapshot.
    *  @return `None` if write was successful, or `Some` current snapshot
    */
  protected def writeReplacement(key: K, oldSnapshot: Snapshot, newSnapshot: Snapshot): Future[Option[Snapshot]]

  /** Write snapshot, if current expectation holds. */
  private def writeIfExpected(key: K, expected: Option[Snapshot], snapshot: Snapshot): Future[Option[Snapshot]] =
    expected match {
      case Some(expected) => writeReplacement(key, expected, snapshot)
      case _ => writeIfAbsent(key, snapshot)
    }

  /**
    * Update/Insert.
    * @param key The key to update
    * @param updateThunk The update function. Return `None` if no insert/update is desired
    * @return The result of the `updateThunk`
    */
  def upsert[R](key: K)(updateThunk: Option[Snapshot] => Future[(Option[Snapshot], R)])(
      implicit ec: ExecutionContext): Future[(Option[Update], R)] = {
    read(key).flatMap(upsertRecursive(key, _, updateThunk))
  }

  /**
   * Snapshot update.
   * @param snapshot The snapshot, content, revision, and tick
   * @param contentUpdated `true` if snapshot content was updated, `false` if only revision and/or tick
   */
  final case class Update(snapshot: Snapshot, contentUpdated: Boolean)

  /** Update and return updated snapshot, if any. */
  private def upsertRecursive[R](
      key: K, existing: Option[Snapshot],
      upsertThunk: Option[Snapshot] => Future[(Option[Snapshot], R)])(
      implicit ec: ExecutionContext): Future[(Option[Update], R)] = {
    upsertThunk(existing).flatMap {
      case (result, payload) =>
      if (result.isEmpty || result == existing) Future successful None -> payload
      else {
        val Some(newSnapshot) = result // Known nonEmpty
        val contentUpdated = existing.forall(_.content != newSnapshot.content)
        val potentialConflict = if (contentUpdated) {
          writeIfExpected(key, existing, newSnapshot)
        } else {
          refresh(key, newSnapshot.revision, newSnapshot.tick).map(_ => None)
        }
        potentialConflict.flatMap {
          case None => Future successful Some(Update(newSnapshot, contentUpdated)) -> payload
          case conflict => upsertRecursive(key, conflict, upsertThunk)
        }
      }
    }
  }

}

private[delta] object StreamProcessStore {
  val UnitFuture = Future.successful(())
  val NoneFuture = Future successful None
}
