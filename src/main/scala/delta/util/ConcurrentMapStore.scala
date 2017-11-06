package delta.util

import scala.collection.Map
import scala.concurrent.Future
import scala.util.Try
import scala.util.control.NonFatal

import scuff.concurrent.Threads

/**
  * Implementation that stores snapshots in a [[scala.collection.concurrent.Map]].
  * Useful when doing batch processing of past events, as a fast in-memory store.
  * If the backing map is either empty or incomplete (this would be expected, to
  * save both memory and load time), provide a fallback lookup mechanism for keys
  * not found.
  * @param cmap The concurrent map implementation
  * @param fallback Optional persistent store fallback, if snapshots are persisted
  */
class ConcurrentMapStore[K, V](
    cmap: collection.concurrent.Map[K, Option[delta.Snapshot[V]]],
    fallback: K => Future[Option[delta.Snapshot[V]]])
  extends StreamProcessStore[K, V] {

  @annotation.tailrec
  private def trySave(key: K, someSnapshot: Some[Snapshot]): Unit = {
    cmap.putIfAbsent(key, someSnapshot) match {
      case None => // Success
      case Some(None) =>
        if (!cmap.replace(key, None, someSnapshot)) {
          trySave(key, someSnapshot)
        }
      case Some(someExisting @ Some(existing)) =>
        val Some(snapshot) = someSnapshot
        if (snapshot.revision > existing.revision || (snapshot.revision == existing.revision && snapshot.tick >= existing.tick)) { // replace with later revision
          if (!cmap.replace(key, someExisting, Some(snapshot))) {
            trySave(key, someSnapshot)
          }
        } else {
          throw Exceptions.writeOlder(key, existing, snapshot)
        }
    }
  }
  def write(key: K, snapshot: Snapshot) = try {
    trySave(key, Some(snapshot))
    StreamProcessStore.UnitFuture
  } catch {
    case NonFatal(th) => Future failed th
  }

  def read(key: K): Future[Option[Snapshot]] = cmap.get(key) match {
    case Some(found) => Future successful found
    case None =>
      fallback(key).map { fallbackResult =>
        cmap.putIfAbsent(key, fallbackResult) getOrElse fallbackResult
      }(Threads.PiggyBack)
  }

  def readBatch(keys: Iterable[K]): Future[Map[K, Snapshot]] = {
      implicit def ec = Threads.PiggyBack
    val readFutures: Seq[Future[Option[(K, Snapshot)]]] =
      keys.map { key =>
        read(key).map {
          _.map(key -> _)
        }
      }.toSeq
    val futureReads: Future[Seq[Option[(K, Snapshot)]]] = Future.sequence(readFutures)
    futureReads.map {
      _.flatten.toMap
    }
  }

  def writeBatch(map: Map[K, Snapshot]): Future[Unit] = {
    map.foreach {
      case (key, snapshot) => write(key, snapshot)
    }
    StreamProcessStore.UnitFuture
  }
  def refresh(key: K, revision: Int, tick: Long): Future[Unit] = {
    cmap.get(key).foreach {
      case Some(snapshot) =>
        write(key, snapshot.copy(revision = revision, tick = tick))
      case _ => sys.error(s"Cannot refresh non-existent key: $key")
    }
    StreamProcessStore.UnitFuture
  }
  def refreshBatch(revisions: Map[K, (Int, Long)]): Future[Unit] = {
    revisions.foreach {
      case (key, (revision, tick)) => refresh(key, revision, tick)
    }
    StreamProcessStore.UnitFuture
  }

  def writeIfAbsent(key: K, snapshot: Snapshot): Future[Option[Snapshot]] = {
    cmap.putIfAbsent(key, Some(snapshot)) match {
      case None => StreamProcessStore.NoneFuture
      case Some(None) =>
        if (cmap.replace(key, None, Some(snapshot))) StreamProcessStore.NoneFuture
        else writeIfAbsent(key, snapshot)
      case Some(existing) => Future successful existing
    }
  }
  def writeReplacement(key: K, oldSnapshot: Snapshot, newSnapshot: Snapshot): Future[Option[Snapshot]] = {
    if (cmap.replace(key, Some(oldSnapshot), Some(newSnapshot))) StreamProcessStore.NoneFuture
    else cmap.get(key) match {
      case Some(existing: Some[_]) => Future successful existing
      case _ => Future fromTry Try(sys.error(s"Cannot refresh non-existent key: $key"))
    }
  }

}
