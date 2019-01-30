package delta.hazelcast

import scala.concurrent.{ Future, Promise }

import com.hazelcast.core.{ EntryEvent, ExecutionCallback, IMap }
import com.hazelcast.map.listener.{ EntryAddedListener, EntryUpdatedListener }

import delta.util.ReadModel
import scuff.Subscription
import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executor

class IMapEntryStateReadModel[ID, S, EVT](
    imap: IMap[ID, EntryState[S, EVT]],
    protected val scheduler: ScheduledExecutorService,
    failureReporter: (Throwable) => Unit,
    protected val defaultLookupTimeout: FiniteDuration = 5555.millis)
  extends ReadModel[ID, S] {

  private type EntryState = delta.hazelcast.EntryState[S, EVT]

  protected def reportFailure(th: Throwable) = failureReporter(th)

  protected def readSnapshot(id: ID)(
      implicit
      ec: ExecutionContext): Future[Option[Snapshot]] = {
    val promise = Promise[Option[Snapshot]]
    val callback = new ExecutionCallback[EntryState] {
      def onResponse(response: EntryState): Unit = {
        if (response != null) promise success Option(response.snapshot)
        else promise success None
      }
      def onFailure(t: Throwable): Unit = promise failure t
    }
    imap.getAsync(id).andThen(callback, new Executor { def execute(r: Runnable) = ec execute r }) 
    promise.future
  }

  protected def subscribe(id: ID)(pf: PartialFunction[SnapshotUpdate, Unit]): Subscription = {
    val entryListener = new EntryAddedListener[ID, EntryState] with EntryUpdatedListener[ID, EntryState] {
      def entryAdded(event: EntryEvent[ID, EntryState]): Unit = handle(event.getValue)
      def entryUpdated(event: EntryEvent[ID, EntryState]): Unit = handle(event.getValue)
      private def handle(entryState: EntryState) = if (entryState != null) entryState.snapshot match {
        case null => // Ignore
        case snapshot =>
          val update = new SnapshotUpdate(snapshot, entryState.contentUpdated)
          if (pf isDefinedAt update) pf(update)
      }
    }
    val regId = imap.addEntryListener(entryListener, id, true /* includeValue */ )
    new Subscription {
      def cancel() = imap.removeEntryListener(regId)
    }
  }

}
