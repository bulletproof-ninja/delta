package delta.hazelcast

import scala.concurrent.{ Future, Promise }
import scala.util.control.NonFatal

import com.hazelcast.core.{ ExecutionCallback, IMap }
import com.hazelcast.logging.ILogger

import delta.SnapshotStore

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
    val p = Promise[Option[Snapshot]]
    f andThen new ExecutionCallback[Snapshot] {
      def onResponse(snapshot: Snapshot) = p.success(Option(snapshot))
      def onFailure(th: Throwable) = p.failure(th)
    }
    p.future
  }

  private def updateFailure(id: K, th: Throwable) = logger.severe(s"""Updating entry $id in "${imap.getName}" failed""", th)

  def write(id: K, snapshot: Snapshot): Future[Unit] = {
    try {
      val promise = Promise[Unit]
      val callback = new ExecutionCallback[Void] {
        def onFailure(th: Throwable) = {
          updateFailure(id, th)
          promise failure th
        }
        def onResponse(v: Void) = promise success Unit
      }
      imap.submitToKey(id, new SnapshotUpdater(Right(snapshot)), callback)
      promise.future
    } catch {
      case NonFatal(th) =>
        updateFailure(id, th)
        Future failed th
    }
  }

}
