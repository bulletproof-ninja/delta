package ulysses.ddd

import concurrent._

/**
 * Trait that stores snapshots in a a [[scala.collection.concurrent.Map]]. Should probably be
 * used in conjunction with a limiter, something to determine if a given
 * revision should be snapshotted, e.g. [[FixedIntervalSnapshots]] or [[LoadTimeSnapshots]],
 * unless memory consumption is a non-issue.
 */
class MapSnapshotStore[AR, ID, EVT, S](map: collection.concurrent.Map[ID, SnapshotStore[S, ID]#Snapshot])(implicit ec: ExecutionContext)
    extends SnapshotStore[S, ID] {

  @annotation.tailrec
  private def trySave(id: ID, snapshot: Snapshot) {
    map.putIfAbsent(id, snapshot) match {
      case None => // Success
      case Some(other) =>
        if (snapshot.revision > other.revision) { // replace with later revision
          if (!map.replace(id, other, snapshot)) {
            trySave(id, snapshot)
          }
        }
    }
  }
  def save(id: ID, snapshot: Snapshot) = Future(trySave(id, snapshot)).onFailure {
    case t => ec.reportFailure(t)
  }
  def load(id: ID): Future[Option[Snapshot]] = Future(map.get(id))

}
