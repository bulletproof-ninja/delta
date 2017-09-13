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
  * Useful when doing batch processing of past events, as a fast in-memory store.
  * If the backing map is either empty or incomplete (this would be expected, to 
  * save both memory and load time), provide a fallback lookup mechanism for keys
  * not found.
  * @param cmap The concurrent map implementation
  * @param fallback Optional persistent store fallback, if snapshots are persisted
  */
class ConcurrentMapSnapshotStore[K, V](
  cmap: collection.concurrent.Map[K, Snapshot[V]],
  fallback: K => Future[Option[Snapshot[V]]] = Function.const(SnapshotStore.NoneFuture) _)
    extends SnapshotStore[K, V] {

  @annotation.tailrec
  private def trySave(key: K, snapshot: Snapshot[V]): Unit = {
    cmap.putIfAbsent(key, snapshot) match {
      case None => // Success
      case Some(existing) =>
        if (snapshot.revision > existing.revision || (snapshot.revision == existing.revision && snapshot.tick >= existing.tick)) { // replace with later revision
          if (!cmap.replace(key, existing, snapshot)) {
            trySave(key, snapshot)
          }
        } else {
          throw SnapshotStore.Exceptions.writeOlderRevision(key, existing, snapshot)
        }
    }
  }
  def write(key: K, snapshot: Snapshot[V]) = try {
    trySave(key, snapshot)
    SnapshotStore.UnitFuture
  } catch {
    case NonFatal(th) => Future failed th
  }

  def read(key: K): Future[Option[Snapshot[V]]] = cmap.get(key) match {
    case None =>
      fallback(key).map {
        _.map { value =>
          cmap.putIfAbsent(key, value) getOrElse value
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
      case (key, snapshot) => write(key, snapshot)
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

}
