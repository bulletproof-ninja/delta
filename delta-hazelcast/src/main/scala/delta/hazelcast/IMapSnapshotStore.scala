package delta.hazelcast

import scala.collection.Map
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.util.control.NonFatal

import com.hazelcast.core.{ ExecutionCallback, IMap }
import com.hazelcast.logging.ILogger

import delta.{ SnapshotStore, Snapshot }

/**
  * [[delta.SnapshotStore]] implementation, backed
  * by a Hazelcast `IMap`.
  * Since `IMap`s are potentially incomplete caches,
  * a specific `maxTick` function is required
  * to resolve the current high-water mark.
  * @tparam K Key type
  * @tparam V Snapshot content type
  * @param imap The `IMap` instance
  * @param maxTickImpl Function for resolving the highest tick
  * @param logger `ILogger` instance
  */
class IMapSnapshotStore[K, V](
  imap: IMap[K, Snapshot[V]],
  logger: ILogger)(implicit ec: ExecutionContext)
    extends SnapshotStore[K, V] {

  def read(id: K): Future[Option[Snapshot[V]]] = {
    val f = imap.getAsync(id)
    val p = Promise[Option[Snapshot[V]]]
    f andThen new ExecutionCallback[Snapshot[V]] {
      def onResponse(snapshot: Snapshot[V]) = p.success(Option(snapshot))
      def onFailure(th: Throwable) = p.failure(th)
    }
    p.future
  }

  private def updateFailure(id: K, th: Throwable) = logger.severe(s"""Updating entry $id in "${imap.getName}" failed""", th)

  def write(id: K, snapshot: Snapshot[V]): Future[Unit] = {
    try {
      val promise = Promise[Unit]
      val callback = new ExecutionCallback[Void] {
        def onFailure(th: Throwable) = {
          updateFailure(id, th)
          promise failure th
        }
        def onResponse(v: Void) = promise success Unit
      }
      imap.submitToKey(id, new SnapshotUpdater(snapshot), callback)
      promise.future
    } catch {
      case NonFatal(th) =>
        updateFailure(id, th)
        Future failed th
    }
  }

  def readBatch(keys: Iterable[K]): Future[Map[K, Snapshot[V]]] = {
    val seqFutures: Seq[Future[(K, Option[Snapshot[V]])]] =
      keys.map { key =>
        this.read(key).map { option => key -> option }
      }.toSeq
    val futureSeq: Future[Seq[(K, Option[Snapshot[V]])]] = Future.sequence(seqFutures)
    futureSeq.map { seq =>
      seq.collect {
        case (key, Some(snapshot)) => key -> snapshot
      }.toMap
    }
  }
  def writeBatch(snapshots: Map[K, Snapshot[V]]): Future[Unit] = {
    val futures = snapshots.toSeq.map {
      case (key, snapshot) => write(key, snapshot)
    }
    Future.sequence(futures).map(_ => Unit)
  }
  def refresh(key: K, revision: Int, tick: Long): Future[Unit] = {
    try {
      val promise = Promise[Unit]
      val callback = new ExecutionCallback[Void] {
        def onFailure(th: Throwable) = {
          updateFailure(key, th)
          promise failure th
        }
        def onResponse(v: Void) = promise success Unit
      }
      imap.submitToKey(key, new SnapshotUpdater(revision, tick), callback)
      promise.future
    } catch {
      case NonFatal(th) =>
        updateFailure(key, th)
        Future failed th
    }

  }
  def refreshBatch(revisions: Map[K, (Int, Long)]): Future[Unit] = {
    val futures = revisions.toSeq.map {
      case (key, (revision, tick)) => refresh(key, revision, tick)
    }
    Future.sequence(futures).map(_ => Unit)
  }

}
