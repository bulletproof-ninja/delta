package delta.process

import delta.Tick

import scala.jdk.CollectionConverters._
import scala.concurrent._
import scala.util.control.NonFatal

import scala.collection.concurrent.{ Map => CMap, TrieMap }

import scuff.concurrent._
import delta.util.Logging

object ConcurrentMapStore {

  def typed[K, S](cmap: CMap[Any, Any] = new TrieMap): CMap[K, ReplayState[S]] =
    cmap.asInstanceOf[CMap[K, ReplayState[S]]]

  def typed[K, S](cmap: java.util.concurrent.ConcurrentMap[Any, Any]): CMap[K, ReplayState[S]] =
    typed(cmap.asScala)

  def asReplayStore[K, S, U](
      temp: CMap[K, ReplayState[S]],
      persistentStore: StreamProcessStore[K, S, U]) =
    new ConcurrentMapStore[K, S, U](
      temp,
      persistentStore.name,
      persistentStore.channels,
      persistentStore.tickWatermark,
      persistentStore.read)(
        UpdateCodec.None)

  def asReplayStore[K, S, U](
      temp: java.util.concurrent.ConcurrentMap[K, ReplayState[S]],
      persistentStore: StreamProcessStore[K, S, U]) =
    new ConcurrentMapStore[K, S, U](
      temp.asScala,
      persistentStore.name,
      persistentStore.channels,
      persistentStore.tickWatermark,
      persistentStore.read)(
        UpdateCodec.None)

  def asReplayStore[K, S, U](
      maxTick: Option[java.lang.Long],
      temp: java.util.concurrent.ConcurrentMap[K, ReplayState[S]],
      name: String,
      channels: Set[delta.Channel],
      readFallback: K => Future[Option[delta.Snapshot[S]]]) =
    new ConcurrentMapStore[K, S, U](
      temp.asScala, name, channels, maxTick.map(_.longValue), readFallback)(
        UpdateCodec.None)

  def asReplayStore[K, S, U](
      temp: CMap[K, ReplayState[S]],
      name: String,
      channels: Set[delta.Channel],
      tickWatermark: Option[Tick])(
      readFallback: K => Future[Option[delta.Snapshot[S]]]) =
    new ConcurrentMapStore[K, S, U](
      temp, name, channels, tickWatermark, readFallback)(
        UpdateCodec.None)

  def apply[K, S, U](
      temp: CMap[K, ReplayState[S]],
      name: String,
      channels: Set[delta.Channel],
      tickWatermark: Option[Tick])(
      readFallback: K => Future[Option[delta.Snapshot[S]]])(
      implicit updateCodec: UpdateCodec[S, U]) =
    new ConcurrentMapStore[K, S, U](temp, name, channels, tickWatermark, readFallback)

  def apply[K, S, U](
      maxTick: Option[java.lang.Long],
      temp: java.util.concurrent.ConcurrentMap[K, ReplayState[S]],
      name: String,
      channels: Set[delta.Channel],
      readFallback: K => Future[Option[delta.Snapshot[S]]])(
      implicit updateCodec: UpdateCodec[S, U]) =
    new ConcurrentMapStore[K, S, U](temp.asScala, name, channels, maxTick.map(_.longValue), readFallback)

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
  cmap: CMap[K, ReplayState[W]],
  val name: String,
  val channels: Set[delta.Channel],
  getTickWatermark: => Option[Tick],
  readThrough: K => Future[Option[delta.Snapshot[W]]])(
  implicit
  protected val updateCodec: UpdateCodec[W, U],
  protected val logging: Logging)
extends StreamProcessStore[K, W, U]
with NonBlockingCASWrites[K, W, U] {

  def tickWatermark: Option[Tick] = getTickWatermark

  private[this] val unknownKeys = new collection.concurrent.TrieMap[K, Unit]

  /**
    * @return Iterator for the snapshots that have been updated
    */
  def updatedSnapshots: Iterator[(K, Snapshot)] =
    cmap.iterator
      .collect {
        case (key, ReplayState(snapshot, true)) => key -> snapshot
      }

  def clear(): Unit = {
    unknownKeys.clear()
    cmap.clear()
  }

  @annotation.tailrec
  private def trySave(key: K, snapshot: Snapshot): Option[Snapshot] = {
    cmap.putIfAbsent(key, ReplayState(snapshot)) match {
      case None => // Success
        unknownKeys.remove(key)
        None
      case Some(value @ ReplayState(existing, _)) =>
        if (snapshot.revision > existing.revision || (snapshot.revision == existing.revision && snapshot.tick >= existing.tick)) { // replace with later revision
          if (!cmap.replace(key, value, ReplayState(snapshot))) {
            trySave(key, snapshot)
          } else None
        } else {
          Some(existing)
        }
    }
  }
  def write(key: K, snapshot: Snapshot) = try {
    trySave(key, snapshot) match {
      case None => // Success
        Future.unit
      case Some(existing) =>
        Future failed Exceptions.writeOlder(key, existing, snapshot)
    }
  } catch {
    case NonFatal(th) => Future failed th
  }

  def read(key: K): Future[Option[Snapshot]] =
    cmap.get(key) match {
      case Some(state) =>
        Future successful Some(state.snapshot)
      case None =>
        if (unknownKeys contains key) Future.none
        else readThrough(key).map {
          case fallbackSnapshot @ Some(snapshot) =>
            cmap
              .putIfAbsent(key, ReplayState(snapshot, updated = false))
              .map(_.snapshot)
              .orElse(fallbackSnapshot)
          case None =>
            unknownKeys.update(key, ())
            None
        }(Threads.PiggyBack)
    }

  def readBatch(keys: IterableOnce[K]): Future[Map[K, Snapshot]] = {
    implicit def ec = Threads.PiggyBack
    val readFutures: Seq[Future[Option[(K, Snapshot)]]] =
      keys.map { key =>
        read(key).map {
          _.map(key -> _)
        }
      }.toSeq
    val futureReads: Future[Seq[Option[(K, Snapshot)]]] = Future sequence readFutures
    futureReads.map {
      _.flatten
      .toMap
    }
  }

  def writeBatch(
      batch: IterableOnce[(K, Snapshot)])
      : Future[WriteErrors] = {
    batch.foreach {
      case (key, snapshot) => write(key, snapshot)
    }
    NoWriteErrorsFuture
  }

  def refresh(key: K, revision: Revision, tick: Tick): Future[Unit] = {
    cmap.get(key).foreach {
      case ReplayState(snapshot, _) =>
        trySave(key, snapshot.copy(revision = revision, tick = tick))
    }
    Future.unit
  }

  def refreshBatch(
      revisions: IterableOnce[(K, Revision, Tick)])
      : Future[WriteErrors] = {
    revisions.iterator.foreach {
      case (key, revision, tick) => refresh(key, revision, tick)
    }
    NoWriteErrorsFuture
  }

  def writeIfAbsent(key: K, snapshot: Snapshot): Future[Option[Snapshot]] = {
    Future successful {
      cmap.putIfAbsent(key, ReplayState(snapshot)) match {
        case None => // put successful
          unknownKeys.remove(key)
          None
        case Some(existingState) => // put unsuccessful
          Some(existingState.snapshot)
      }
    }
  }

  def writeReplacement(key: K, oldSnapshot: Snapshot, newSnapshot: Snapshot): Future[Option[Snapshot]] = {
    cmap.get(key) match {
      case Some(oldValue) if (oldValue.snapshot eq oldSnapshot) &&
        cmap.replace(key, oldValue, ReplayState(newSnapshot)) =>
        Future.none
      case Some(nonMatching) =>
        Future successful Some(nonMatching.snapshot)
      case None =>
        Future failed new IllegalStateException(s"Cannot refresh non-existent key: $key")
    }
  }

}
