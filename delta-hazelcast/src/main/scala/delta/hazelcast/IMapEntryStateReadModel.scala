package delta.hazelcast

import scala.concurrent.{ Future, Promise }

import com.hazelcast.core.{ EntryEvent, ExecutionCallback, IMap }
import com.hazelcast.map.listener.{ EntryAddedListener, EntryUpdatedListener }

import delta.read._
import scuff.Subscription
import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executor

class IMapEntryStateReadModel[ID, S, EVT](
    imap: IMap[ID, EntryState[S, EVT]],
    protected val scheduler: ScheduledExecutorService,
    failureReporter: (Throwable) => Unit,
    defaultReadTimeout: FiniteDuration = DefaultReadTimeout)
  extends BasicReadModel[ID, S]
  with SubscriptionSupport[ID, S] {

  private type EntryState = delta.hazelcast.EntryState[S, EVT]

  protected def reportFailure(th: Throwable) = failureReporter(th)

  protected def readAgain(id: ID, expected: Either[Long, Int])(
      implicit
      ec: ExecutionContext): Future[Snapshot] =
    expected match {
      case Right(minRev) => read(id).flatMap(verifyRevision(id, _, minRev))
      case Left(minTick) => read(id).flatMap(verifyTick(id, _, minTick))
    }

  def read(id: ID)(
      implicit
      ec: ExecutionContext): Future[Snapshot] = {
    val promise = Promise[Option[Snapshot]]
    val callback = new ExecutionCallback[EntryState] {
      def onResponse(response: EntryState): Unit = {
        if (response != null) promise success Option(response.snapshot)
        else promise success None
      }
      def onFailure(t: Throwable): Unit = promise failure t
    }
    val exec = ec match {
      case exec: Executor => exec
      case _ => new Executor { def execute(r: Runnable) = ec execute r }
    }
    imap.getAsync(id).andThen(callback, exec)
    promise.future.flatMap {
      verify(id, _)
    }
  }

  def readMinTick(id: ID, minTick: Long)(
      implicit
      ec: ExecutionContext): Future[Snapshot] = readMinTick(id, minTick, defaultReadTimeout)

  def readMinRevision(id: ID, minRevision: Int)(
      implicit
      ec: ExecutionContext): Future[Snapshot] = readMinRevision(id, minRevision, defaultReadTimeout)

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
