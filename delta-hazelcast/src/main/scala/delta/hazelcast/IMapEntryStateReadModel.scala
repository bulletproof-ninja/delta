package delta.hazelcast

import scala.concurrent.{ Future, Promise }

import com.hazelcast.core.{ EntryEvent, ExecutionCallback, IMap }
import com.hazelcast.map.listener.{ EntryAddedListener, EntryUpdatedListener }

import delta.read._
import scuff.Subscription
import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import com.hazelcast.map.listener.EntryMergedListener
import delta.process.UpdateCodec

object IMapEntryStateReadModel {

  import Predef.{ implicitly => ? }

  def apply[ID, S, U](
      imap: IMap[ID, _ <: EntryState[S, _]],
      scheduler: ScheduledExecutorService,
      failureReporter: Throwable => Unit)(
      implicit updateCodec: UpdateCodec[S, U]) =
    new IMapEntryStateReadModel[ID, S, ID, S, U](
      imap, scheduler, failureReporter, DefaultReadTimeout)(
      updateCodec, ?, ?, Option(_))

  def apply[ID, S, U](
      updateCodec: UpdateCodec[S, U],
      imap: IMap[ID, _ <: EntryState[S, _]],
      scheduler: ScheduledExecutorService,
      failureReporter: Throwable => Unit) =
    new IMapEntryStateReadModel[ID, S, ID, S, U](
      imap, scheduler, failureReporter, DefaultReadTimeout)(
      updateCodec, ?, ?, Option(_))

  def apply[ID, S, U](
      imap: IMap[ID, _ <: EntryState[S, _]],
      scheduler: ScheduledExecutorService,
      failureReporter: Throwable => Unit,
      defaultReadTimeout: FiniteDuration)(
      implicit updateCodec: UpdateCodec[S, U]) =
    new IMapEntryStateReadModel[ID, S, ID, S, U](
      imap, scheduler, failureReporter, defaultReadTimeout)(
      updateCodec, ?, ?, Option(_))

  def apply[ID, S, U](
      updateCodec: UpdateCodec[S, U],
      imap: IMap[ID, _ <: EntryState[S, _]],
      scheduler: ScheduledExecutorService,
      failureReporter: Throwable => Unit,
      defaultReadTimeout: FiniteDuration) =
    new IMapEntryStateReadModel[ID, S, ID, S, U](
      imap, scheduler, failureReporter, defaultReadTimeout)(
      updateCodec, ?, ?, Option(_))

  def apply[ID, S](
      imap: IMap[ID, _ <: EntryState[S, _]],
      scheduler: ScheduledExecutorService,
      failureReporter: Throwable => Unit,
      defaultReadTimeout: FiniteDuration = DefaultReadTimeout) =
    new IMapEntryStateReadModel[ID, S, ID, S, S](
      imap, scheduler, failureReporter, defaultReadTimeout)(
      ?, ?, ?, Option(_))

}

class IMapEntryStateReadModel[ID, S, MID, ES, U](
  protected val imap: IMap[MID, _ <: EntryState[ES, _]],
  protected val scheduler: ScheduledExecutorService,
  failureReporter: Throwable => Unit,
  protected val defaultReadTimeout: FiniteDuration = DefaultReadTimeout)(
  implicit
  updateCodec: UpdateCodec[ES, U],
  toMapKey: ID => MID,
  toView: (ID, ES) => S,
  fromView: S => Option[ES])
extends BasicReadModel[ID, S]
with SubscriptionSupport[ID, S, U] {

  protected type StreamId = MID
  protected def StreamId(id: ID) = toMapKey(id)

  private type EntryState = delta.hazelcast.EntryState[ES, _]

  protected def reportFailure(th: Throwable) =
    failureReporter(th)

  protected def updateState(id: ID, prev: Option[S], update: U): Option[S] = {
    val updated = prev match {
      case None => updateCodec.asSnapshot(None, update)
      case Some(prev) => fromView(prev) match {
        case None => None
        case prev => updateCodec.asSnapshot(prev, update)
      }
    }
    updated.map(toView(id, _))
  }

  protected def readSnapshot(id: ID)(
    implicit ec: ExecutionContext): Future[Option[Snapshot]] = {

    val promise = Promise[Option[Snapshot]]
    val callback = new ExecutionCallback[Snapshot] {
      def onResponse(snapshot: Snapshot): Unit = promise success Option(snapshot)
      def onFailure(t: Throwable): Unit = promise failure t
    }
    val reader = new EntryStateSnapshotReader[ES, S](toView(id, _))
    imap.submitToKey(id, reader, callback)
    promise.future
  }

  protected def readAgain(id: ID, minRevision: Int, minTick: Long)(
      implicit
      ec: ExecutionContext): Future[Option[Snapshot]] =
    readSnapshot(id)

  protected def subscribe(id: ID)(callback: Update => Unit): Subscription = {
    val entryListener =
      new EntryAddedListener[ID, EntryState]
      with EntryUpdatedListener[ID, EntryState]
      with EntryMergedListener[ID, EntryState] {
        def entryAdded(event: EntryEvent[ID, EntryState]): Unit = onUpsert(None, event.getValue)
        def entryUpdated(event: EntryEvent[ID, EntryState]): Unit = onUpsert(Option(event.getOldValue), event.getValue)
        def entryMerged(event: EntryEvent[ID,EntryState]): Unit = onUpsert(Option(event.getOldValue), event.getValue)
        private def onUpsert(prevState: Option[EntryState], entryState: EntryState): Unit = {
          if (entryState != null) entryState.snapshot match {
            case null => // Ignore
            case currSnapshot =>
              val prevSnapshot = prevState match {
                case Some(EntryState(snapshot, _, _)) => Option(snapshot)
                case _ => None
              }
              val update = updateCodec.asUpdate(prevSnapshot, currSnapshot, entryState.contentUpdated)
              callback(update)
          }
        }
      }
    val regId = imap.addEntryListener(entryListener, id, /* includeValue */ true)
    new Subscription {
      def cancel() = imap.removeEntryListener(regId)
    }
  }

}
