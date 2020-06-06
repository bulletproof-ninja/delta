package delta.hazelcast

import collection.Map
import scala.concurrent.Future
import scala.util.control.NonFatal

import com.hazelcast.core.IMap
import com.hazelcast.logging.ILogger

import delta._
import delta.process._

import scuff.concurrent.Threads

/**
  * [[delta.SnapshotStore]] implementation,
  * backed by a Hazelcast `IMap`.
  * @tparam K Key type
  * @tparam V Snapshot content type
  * @param imap The `IMap` instance
  * @param logger `ILogger` instance
  */
class IMapSnapshotStore[K, V](
    imap: IMap[K, delta.Snapshot[V]],
    logger: ILogger)
  extends SnapshotStore[K, V] {

  def read(id: K): Future[Option[Snapshot]] = {
    val f = imap.getAsync(id)
    val callback = CallbackPromise.option[Snapshot]
    f andThen callback
    callback.future
  }

  private def updateFailure(id: K, th: Throwable) =
    logger.severe(s"""Updating entry $id in "${imap.getName}" failed""", th)

  def write(id: K, snapshot: Snapshot): Future[Unit] =
    write(id, Right(snapshot))

  protected def write(id: K, update: Either[(Int, Long), Snapshot]): Future[Unit] =
    try {
      val callback = new CallbackPromise[Any, Unit](_ => ()) {
        override def onFailure(th: Throwable) = {
          updateFailure(id, th)
          super.onFailure(th)
        }
      }
      imap.submitToKey(id, new SnapshotUpdater(update), callback)
      callback.future
    } catch {
      case NonFatal(th) =>
        updateFailure(id, th)
        Future failed th
    }

}

abstract class IMapStreamProcessStore[K, V, U](
    imap: IMap[K, delta.Snapshot[V]],
    logger: ILogger)
  extends IMapSnapshotStore[K, V](imap, logger)
  with StreamProcessStore[K, V, U]
  with NonBlockingCASWrites[K, V, U] {

  def readBatch(keys: Iterable[K]): Future[Map[K, Snapshot]] = {
    implicit def ec = Threads.PiggyBack
    val futures = keys.map { key =>
      val f = this.read(key).map(_.map(key -> _))
      f
    }
    Future.sequence(futures).map(_.flatten.toMap)
  }
  def writeBatch(batch: Map[K, Snapshot]): Future[Unit] = {
    implicit def ec = Threads.PiggyBack
    val futures = batch.iterator.map {
      case (key, snapshot) => write(key, snapshot)
    }
    Future.sequence(futures).map(_ => ())
  }
  def refresh(key: K, revision: Revision, tick: Tick): Future[Unit] =
    write(key, Left(revision -> tick))

  def refreshBatch(revisions: Map[K, (Revision, Tick)]): Future[Unit] = {
    implicit def ec = Threads.PiggyBack
    val futures = revisions.iterator.map {
      case (key, update) => write(key, Left(update))
    }
    Future.sequence(futures).map(_ => ())
  }

  import IMapStreamProcessStore._

  /**
   *  Write snapshot, if absent.
   *  Otherwise return present snapshot.
   *  @return `None` if write was successful, or `Some` present snapshot
   */
  protected def writeIfAbsent(
    key: K, snapshot: Snapshot): Future[Option[Snapshot]] = {

    val entryProc = new WriteIfAbsent(snapshot)
    val callback = CallbackPromise.option[Snapshot]
    imap.submitToKey(key, entryProc, callback)
    callback.future
  }

  /**
   *  Write replacement snapshot, if old snapshot matches.
   *  Otherwise return current snapshot.
   *  @return `None` if write was successful, or `Some` current snapshot
   */
  protected def writeReplacement(
    key: K, oldSnapshot: Snapshot, newSnapshot: Snapshot)
    : Future[Option[Snapshot]] = {

    val entryProc = new WriteReplacement(oldSnapshot.revision, oldSnapshot.tick, newSnapshot)
    val callback = CallbackPromise.option[Snapshot]
    imap.submitToKey(key, entryProc, callback)
    callback.future
  }

}

object IMapStreamProcessStore {

  import com.hazelcast.map.AbstractEntryProcessor
  import delta.Snapshot
  import java.util.Map.Entry

  sealed abstract class Updater[K, V]
    extends AbstractEntryProcessor[K, Snapshot[V]](true)

  final case class WriteIfAbsent[K, V](snapshot: Snapshot[V])
    extends Updater[K, V] {

    def process(entry: Entry[K, Snapshot[V]]): Object =
      entry.getValue match {
        case null =>
          entry setValue snapshot
          null
        case existing =>
          existing
      }

    }

  final case class WriteReplacement[K, V](
      expectedRev: Revision, expectedTick: Tick, newSnapshot: Snapshot[V])
    extends Updater[K, V] {

    def process(entry: Entry[K, Snapshot[V]]): Object =
      entry.getValue match {
        case null => throw new IllegalStateException(s"No snapshot found!")
        case existing @ Snapshot(_, rev, tick) =>
          if (rev == expectedRev && tick == expectedTick) {
            entry setValue newSnapshot
            null
          } else existing
      }

    }

}
