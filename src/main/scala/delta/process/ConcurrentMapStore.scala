package delta.process

import collection.Map
import collection.JavaConverters._
import scala.concurrent.Future
import scala.util.control.NonFatal
import scuff.concurrent.Threads

object ConcurrentMapStore {

  case class Value[T](snapshot: delta.Snapshot[T], modified: Boolean = true)

  def apply[K, V](
    persistentStore: StreamProcessStore[K, V],
    temp: collection.concurrent.Map[K, ConcurrentMapStore.Value[V]]) =
      new ConcurrentMapStore(temp, persistentStore.tickWatermark, persistentStore.read)

  def apply[K, V](
    persistentStore: StreamProcessStore[K, V],
    temp: java.util.concurrent.ConcurrentMap[K, ConcurrentMapStore.Value[V]]) =
      new ConcurrentMapStore(temp.asScala, persistentStore.tickWatermark, persistentStore.read)

  def apply[K, V](
    maxTick: Option[java.lang.Long],
    temp: java.util.concurrent.ConcurrentMap[K, ConcurrentMapStore.Value[V]],
    readFallback: K => Future[Option[delta.Snapshot[V]]]) =
      new ConcurrentMapStore(temp.asScala, maxTick.map(_.longValue), readFallback)

  def apply[K, V](
    temp: collection.concurrent.Map[K, ConcurrentMapStore.Value[V]],
    tickWatermark: Option[Long])(
    readFallback: K => Future[Option[delta.Snapshot[V]]]) =
      new ConcurrentMapStore(temp, tickWatermark, readFallback)
}

/**
 * [[delta.process.StreamProcessStore]] implementation that stores snapshots in a
 * [[scala.collection.concurrent.Map]].
 * Useful when doing replay processing of past events, as a fast local memory store.
 * If the backing map is either empty or incomplete (this would be expected, to
 * save both memory and load time), provide a fallback lookup mechanism for keys
 * not found.
 * NOTE: This implementation IS NOT a two-way cache. There's no mechanism
 * to write through.
 * @param cmap The concurrent map implementation
 * @param lookupFallback Persistent store fallback
 */
final class ConcurrentMapStore[K, V] private (
  cmap: collection.concurrent.Map[K, ConcurrentMapStore.Value[V]],
  val tickWatermark: Option[Long],
  readFallback: K => Future[Option[delta.Snapshot[V]]])
  extends StreamProcessStore[K, V] with NonBlockingCASWrites[K, V] {

  import ConcurrentMapStore.Value

  private[this] val unknownKeys = new collection.concurrent.TrieMap[K, Unit]

  def modifiedSnapshots: Iterator[(K, Snapshot)] =
    cmap.iterator
      .collect {
        case (key, Value(snapshot, true)) => key -> snapshot
      }

  def clear() = {
    cmap.clear()
    unknownKeys.clear()
  }

  @annotation.tailrec
  private def trySave(key: K, snapshot: Snapshot): Option[Snapshot] = {
    cmap.putIfAbsent(key, Value(snapshot)) match {
      case None => // Success
        unknownKeys.remove(key)
        None
      case Some(value @ Value(existing, _)) =>
        if (snapshot.revision > existing.revision || (snapshot.revision == existing.revision && snapshot.tick >= existing.tick)) { // replace with later revision
          if (!cmap.replace(key, value, Value(snapshot))) {
            trySave(key, snapshot)
          } else None
        } else {
          Some(existing)
        }
    }
  }
  def write(key: K, snapshot: Snapshot) = try {
    trySave(key, snapshot) match {
      case None =>
        StreamProcessStore.UnitFuture
      case Some(existing) =>
        Future failed Exceptions.writeOlder(key, existing, snapshot)
    }
  } catch {
    case NonFatal(th) => Future failed th
  }

  def read(key: K): Future[Option[Snapshot]] = cmap.get(key) match {
    case found @ Some(_) => Future successful found.map(_.snapshot)
    case None =>
      if (unknownKeys contains key) StreamProcessStore.NoneFuture
      else readFallback(key).map {

        case fallbackSnapshot @ Some(snapshot) =>
          cmap.putIfAbsent(key, Value(snapshot, modified = false))
            .map(_.snapshot) orElse fallbackSnapshot

        case None =>
          unknownKeys.update(key, ())
          None

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
      case Value(snapshot, _) =>
        trySave(key, snapshot.copy(revision = revision, tick = tick))
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
    Future successful cmap.putIfAbsent(key, Value(snapshot)).map(_.snapshot)
  }

  def writeReplacement(key: K, oldSnapshot: Snapshot, newSnapshot: Snapshot): Future[Option[Snapshot]] = {
    cmap.get(key) match {
      case Some(oldValue) if (oldValue.snapshot eq oldSnapshot) &&
        cmap.replace(key, oldValue, Value(newSnapshot)) =>
        StreamProcessStore.NoneFuture
      case Some(nonMatching) =>
        Future successful Some(nonMatching.snapshot)
      case None =>
        Future failed new IllegalStateException(s"Cannot refresh non-existent key: $key")
    }
  }

}
