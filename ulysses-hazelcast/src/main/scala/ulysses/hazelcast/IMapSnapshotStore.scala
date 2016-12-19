package ulysses.hazelcast

import scala.concurrent.{ Future, Promise }
import scala.util.control.NonFatal

import com.hazelcast.core.{ ExecutionCallback, IMap }
import com.hazelcast.logging.ILogger

import ulysses.ddd.SnapshotStore

class IMapSnapshotStore[ID, S <: AnyRef](
  imap: IMap[ID, SnapshotStore.Snapshot[S]])(implicit logger: ILogger)
    extends SnapshotStore[ID, S] {

  def load(id: ID): Future[Option[Snapshot]] = {
    val f = imap.getAsync(id)
    val p = Promise[Option[Snapshot]]
    f andThen new ExecutionCallback[Snapshot] {
      def onResponse(snapshot: Snapshot) = p.success(Option(snapshot))
      def onFailure(th: Throwable) = p.failure(th)
    }
    p.future
  }

  def save(id: ID, snapshot: Snapshot): Unit = {
      def saveFailure(id: ID, th: Throwable) = logger.severe(s"Saving snapshot $id failed", th)
    try {
      val f = imap.setAsync(id, snapshot)
      f andThen new ExecutionCallback[Void] {
        def onResponse(void: Void) = ()
        def onFailure(th: Throwable) = saveFailure(id, th)
      }
    } catch {
      case NonFatal(th) => saveFailure(id, th)
    }
  }

  override def assumeSnapshotCurrent = true

}
