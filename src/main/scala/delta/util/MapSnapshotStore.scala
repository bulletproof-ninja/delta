package delta.util

import collection.Map
import scala.concurrent.Future
import scala.util.control.NonFatal

import delta.{ SnapshotStore, Snapshot }

/**
  * Trait that stores snapshots in a [[scala.collection.concurrent.Map]].
  */
class MapSnapshotStore[K, V >: Null](
  cmap: collection.concurrent.Map[K, Snapshot[V]])
    extends SnapshotStore[K, V] {

  @annotation.tailrec
  private def trySave(id: K, snapshot: Snapshot[V]) {
    cmap.putIfAbsent(id, snapshot) match {
      case None => // Success
      case Some(other) =>
        if (snapshot.revision > other.revision) { // replace with later revision
          if (!cmap.replace(id, other, snapshot)) {
            trySave(id, snapshot)
          }
        }
    }
  }
  def set(id: K, snapshot: Snapshot[V]) = try {
    trySave(id, snapshot)
    SnapshotStore.UnitFuture
  } catch {
    case NonFatal(th) => Future failed th
  }
  def get(id: K): Future[Option[Snapshot[V]]] = Future successful cmap.get(id)

  def getAll(keys: Iterable[K]): Future[Map[K, Snapshot[V]]] = {
    Future successful keys.flatMap(key => cmap.get(key).map(key -> _)).toMap
  }
  def setAll(map: Map[K, Snapshot[V]]): Future[Unit] = {
    map.foreach {
      case (id, snapshot) => set(id, snapshot)
    }
    SnapshotStore.UnitFuture
  }
  def update(key: K, revision: Int, tick: Long): Future[Unit] = {
    cmap.get(key).foreach { snapshot =>
      set(key, snapshot.copy(revision = revision, tick = tick))
    }
    SnapshotStore.UnitFuture
  }
  def updateAll(revisions: Map[K, (Int, Long)]): Future[Unit] = {
    revisions.foreach {
      case (key, (revision, tick)) => update(key, revision, tick)
    }
    SnapshotStore.UnitFuture
  }

}
