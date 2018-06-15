package delta.hazelcast

import scala.concurrent.Future
import scala.util.control.NonFatal

import com.hazelcast.core.IMap
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
    val callback = CallbackPromise.option[Snapshot]
    f andThen callback
    callback.future
  }

  private def updateFailure(id: K, th: Throwable) = logger.severe(s"""Updating entry $id in "${imap.getName}" failed""", th)

  def write(id: K, snapshot: Snapshot): Future[Unit] = {
    try {
      val callback = new CallbackPromise[Any, Unit](_ => ()) {
        override def onFailure(th: Throwable) = {
          updateFailure(id, th)
          super.onFailure(th)
        }
      }
      imap.submitToKey(id, new SnapshotUpdater(Right(snapshot)), callback)
      callback.future
    } catch {
      case NonFatal(th) =>
        updateFailure(id, th)
        Future failed th
    }
  }

}
