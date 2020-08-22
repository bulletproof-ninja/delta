package delta.process

import delta.Tick

import scala.jdk.CollectionConverters._
import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.collection.Map
import scala.collection.concurrent.{ Map => CMap, TrieMap }

import scuff.concurrent._

object ConcurrentMapStore {

  def typed[K, S](cmap: CMap[Any, Any] = new TrieMap): CMap[K, State[S]] =
    cmap.asInstanceOf[CMap[K, State[S]]]

  def typed[K, S](cmap: java.util.concurrent.ConcurrentMap[Any, Any]): CMap[K, State[S]] =
    typed(cmap.asScala)

  final case class State[S](snapshot: delta.Snapshot[S], modified: Boolean = true)

  def asReplayStore[K, S, U](
      temp: CMap[K, ConcurrentMapStore.State[S]],
      persistentStore: StreamProcessStore[K, S, U]) =
    new ConcurrentMapStore[K, S, U](
      temp, persistentStore.name, persistentStore.tickWatermark, persistentStore.read)(
      UpdateCodec.None)

  def asReplayStore[K, S, U](
      temp: java.util.concurrent.ConcurrentMap[K, ConcurrentMapStore.State[S]],
      persistentStore: StreamProcessStore[K, S, U]) =
    new ConcurrentMapStore[K, S, U](
      temp.asScala, persistentStore.name, persistentStore.tickWatermark, persistentStore.read)(
      UpdateCodec.None)

  def asReplayStore[K, S, U](
      maxTick: Option[java.lang.Long],
      temp: java.util.concurrent.ConcurrentMap[K, ConcurrentMapStore.State[S]],
      name: String,
      readFallback: K => Future[Option[delta.Snapshot[S]]]) =
    new ConcurrentMapStore[K, S, U](
      temp.asScala, name, maxTick.map(_.longValue), readFallback)(
      UpdateCodec.None)

  def asReplayStore[K, S, U](
      temp: CMap[K, ConcurrentMapStore.State[S]],
      name: String,
      tickWatermark: Option[Tick])(
      readFallback: K => Future[Option[delta.Snapshot[S]]]) =
    new ConcurrentMapStore[K, S, U](
      temp, name, tickWatermark, readFallback)(
      UpdateCodec.None)

  def apply[K, S, U](
      temp: CMap[K, ConcurrentMapStore.State[S]],
      name: String,
      tickWatermark: Option[Tick])(
      readFallback: K => Future[Option[delta.Snapshot[S]]])(
      implicit updateCodec: UpdateCodec[S, U]) =
    new ConcurrentMapStore[K, S, U](temp, name, tickWatermark, readFallback)

  def apply[K, S, U](
      maxTick: Option[java.lang.Long],
      temp: java.util.concurrent.ConcurrentMap[K, ConcurrentMapStore.State[S]],
      name: String,
      readFallback: K => Future[Option[delta.Snapshot[S]]])(
      implicit updateCodec: UpdateCodec[S, U]) =
    new ConcurrentMapStore[K, S, U](temp.asScala, name, maxTick.map(_.longValue), readFallback)

}

/**
 * [[delta.process.StreamProcessStore]] implementation that stores snapshots in a
 * `scala.collection.concurrent.Map`.
 * Useful when doing replay processing of past events, as a fast local memory store.
 * If the backing map is either empty or incomplete (this would be expected, to
 * save both memory and load time), provide a fallback lookup mechanism for keys
 * not found.
 * @note This implementation IS NOT a two-way cache. There's no mechanism
 * to write through.
 * @param cmap The concurrent map implementation
 * @param readThrough Persistent store fallback
 */
final class ConcurrentMapStore[K, W, U] private (
  cmap: CMap[K, ConcurrentMapStore.State[W]],
  val name: String,
  val tickWatermark: Option[Tick],
  readThrough: K => Future[Option[delta.Snapshot[W]]])(
  implicit protected val updateCodec: UpdateCodec[W, U])
extends StreamProcessStore[K, W, U]
with NonBlockingCASWrites[K, W, U] {

  import ConcurrentMapStore.State

  private[this] val unknownKeys = new collection.concurrent.TrieMap[K, Unit]

  def modifiedSnapshots: Iterator[(K, Snapshot)] =
    cmap.iterator
      .collect {
        case (key, State(snapshot, true)) => key -> snapshot
      }

  def clear() = {
    cmap.clear()
    unknownKeys.clear()
  }

  @annotation.tailrec
  private def trySave(key: K, snapshot: delta.Snapshot[W]): Option[delta.Snapshot[W]] = {
    cmap.putIfAbsent(key, State(snapshot)) match {
      case None => // Success
        unknownKeys.remove(key)
        None
      case Some(value @ State(existing, _)) =>
        if (snapshot.revision > existing.revision || (snapshot.revision == existing.revision && snapshot.tick >= existing.tick)) { // replace with later revision
          if (!cmap.replace(key, value, State(snapshot))) {
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
        Future.unit
      case Some(existing) =>
        Future failed Exceptions.writeOlder(key, existing, snapshot)
    }
  } catch {
    case NonFatal(th) => Future failed th
  }

  def read(key: K): Future[Option[Snapshot]] = cmap.get(key) match {
    case found @ Some(_) => Future successful found.map(_.snapshot)
    case None =>
      if (unknownKeys contains key) Future.none
      else readThrough(key).map {

        case fallbackSnapshot @ Some(snapshot) =>
          cmap.putIfAbsent(key, State(snapshot, modified = false))
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
    Future.unit
  }
  def refresh(key: K, revision: Revision, tick: Tick): Future[Unit] = {
    cmap.get(key).foreach {
      case State(snapshot, _) =>
        trySave(key, snapshot.copy(revision = revision, tick = tick))
    }
    Future.unit
  }
  def refreshBatch(revisions: Map[K, (Revision, Tick)]): Future[Unit] = {
    revisions.foreach {
      case (key, (revision, tick)) => refresh(key, revision, tick)
    }
    Future.unit
  }

  def writeIfAbsent(key: K, snapshot: Snapshot): Future[Option[Snapshot]] = {
    Future successful cmap.putIfAbsent(key, State(snapshot)).map(_.snapshot)
  }

  def writeReplacement(key: K, oldSnapshot: Snapshot, newSnapshot: Snapshot): Future[Option[Snapshot]] = {
    cmap.get(key) match {
      case Some(oldValue) if (oldValue.snapshot eq oldSnapshot) &&
        cmap.replace(key, oldValue, State(newSnapshot)) =>
        Future.none
      case Some(nonMatching) =>
        Future successful Some(nonMatching.snapshot)
      case None =>
        Future failed new IllegalStateException(s"Cannot refresh non-existent key: $key")
    }
  }

}
