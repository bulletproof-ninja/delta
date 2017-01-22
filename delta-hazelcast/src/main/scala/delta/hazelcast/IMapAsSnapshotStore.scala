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
  */
class IMapAsSnapshotStore[K, V](
  imap: IMap[K, Snapshot[V]],
  logger: ILogger)(implicit ec: ExecutionContext)
    extends SnapshotStore[K, V] {

  def get(id: K): Future[Option[Snapshot[V]]] = {
    val f = imap.getAsync(id)
    val p = Promise[Option[Snapshot[V]]]
    f andThen new ExecutionCallback[Snapshot[V]] {
      def onResponse(snapshot: Snapshot[V]) = p.success(Option(snapshot))
      def onFailure(th: Throwable) = p.failure(th)
    }
    p.future
  }

  private def updateFailure(id: K, th: Throwable) = logger.severe(s"""Updating entry $id in "${imap.getName}" failed""", th)

  def set(id: K, snapshot: Snapshot[V]): Future[Unit] = {
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

  def getAll(keys: Iterable[K]): Future[Map[K, Snapshot[V]]] = {
    val seqFutures: Seq[Future[(K, Option[Snapshot[V]])]] =
      keys.map { key =>
        this.get(key).map { option => key -> option }
      }.toSeq
    val futureSeq: Future[Seq[(K, Option[Snapshot[V]])]] = Future.sequence(seqFutures)
    futureSeq.map { seq =>
      seq.collect {
        case (key, Some(snapshot)) => key -> snapshot
      }.toMap
    }
  }
  def setAll(snapshots: Map[K, Snapshot[V]]): Future[Unit] = {
    val futures = snapshots.toSeq.map {
      case (key, snapshot) => set(key, snapshot)
    }
    Future.sequence(futures).map(_ => Unit)
  }
  def update(key: K, revision: Int, tick: Long): Future[Unit] = {
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
  def updateAll(revisions: Map[K, (Int, Long)]): Future[Unit] = {
    val futures = revisions.toSeq.map {
      case (key, (revision, tick)) => update(key, revision, tick)
    }
    Future.sequence(futures).map(_ => Unit)
  }

}
