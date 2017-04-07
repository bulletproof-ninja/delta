package delta.util

import scala.concurrent.Future
import scala.util.Try
import scala.util.control.NonFatal

import scala.collection.Map
import delta.{ Snapshot, SnapshotStore }
import scuff.ScuffTry
import scuff.concurrent.{ ScuffScalaFuture, Threads }

/**
  * Implementation that stores snapshots in a [[scala.collection.concurrent.Map]].
  */
class ConcurrentMapSnapshotStore[K, V](
  cmap: collection.concurrent.Map[K, Snapshot[V]],
  fallback: K => Future[Option[Snapshot[V]]] = Function.const(SnapshotStore.NoneFuture) _)
    extends SnapshotStore[K, V] {

  @annotation.tailrec
  private def trySave(id: K, snapshot: Snapshot[V]) {
    cmap.putIfAbsent(id, snapshot) match {
      case None => // Success
      case Some(existing) =>
        if (snapshot.revision > existing.revision) { // replace with later revision
          if (!cmap.replace(id, existing, snapshot)) {
            trySave(id, snapshot)
          }
        }
    }
  }
  def write(id: K, snapshot: Snapshot[V]) = try {
    trySave(id, snapshot)
    SnapshotStore.UnitFuture
  } catch {
    case NonFatal(th) => Future failed th
  }

  def read(id: K): Future[Option[Snapshot[V]]] = cmap.get(id) match {
    case None =>
      fallback(id).map {
        _.map { value =>
          cmap.putIfAbsent(id, value) getOrElse value
        }
      }(Threads.PiggyBack)
    case found => Future successful found
  }

  def readBatch(keys: Iterable[K]): Future[Map[K, Snapshot[V]]] = Try {
    keys.map(key => key -> read(key)).foldLeft(Map.empty[K, Snapshot[V]]) {
      case (map, (key, future)) =>
        future.await match {
          case Some(snapshot) => map.updated(key, snapshot)
          case _ => map
        }
    }
  }.toFuture

  def writeBatch(map: Map[K, Snapshot[V]]): Future[Unit] = {
    map.foreach {
      case (id, snapshot) => write(id, snapshot)
    }
    SnapshotStore.UnitFuture
  }
  def refresh(key: K, revision: Int, tick: Long): Future[Unit] = {
    cmap.get(key).foreach { snapshot =>
      write(key, snapshot.copy(revision = revision, tick = tick))
    }
    SnapshotStore.UnitFuture
  }
  def refreshBatch(revisions: Map[K, (Int, Long)]): Future[Unit] = {
    revisions.foreach {
      case (key, (revision, tick)) => refresh(key, revision, tick)
    }
    SnapshotStore.UnitFuture
  }

  def maxTick: Future[Option[Long]] = {
    if (cmap.isEmpty) SnapshotStore.NoneFuture
    else Future successful Some(cmap.values.maxBy(_.tick).tick)
  }

}
