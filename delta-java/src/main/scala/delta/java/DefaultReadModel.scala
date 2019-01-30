package delta.java

import delta.SnapshotStore
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import delta.MessageHub
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import scuff.concurrent.Threads
import scala.concurrent.ExecutionContext
import scuff.Subscription
import java.util.concurrent.Executor
import java.util.function.BiConsumer

abstract class DefaultReadModel[ID, SS, S >: SS] protected (
    protected val stateClass: Class[S],
    protected val snapshotStore: SnapshotStore[ID, SS],
    override protected val defaultReadTimeout: FiniteDuration,
    protected val updateHub: MessageHub[(ID, delta.util.SnapshotUpdate[S])],
    protected val updateHubNamespace: MessageHub.Namespace,
    protected val scheduler: ScheduledExecutorService)
  extends delta.util.ReadModel[ID, S]
  with delta.util.ReadModel.MessageHubSupport[ID, S]
  with delta.util.ReadModel.SnapshotStoreSupport[ID, SS, S] {

  protected def this(
      stateClass: Class[S],
      store: SnapshotStore[ID, SS],
      defaultLookupTimeoutLength: Long, defaultLookupTimeoutUnits: TimeUnit,
      hub: MessageHub[(ID, delta.util.SnapshotUpdate[S])],
      hubNS: MessageHub.Namespace,
      scheduler: ScheduledExecutorService) =
    this(stateClass, store, FiniteDuration(defaultLookupTimeoutLength, defaultLookupTimeoutUnits), hub, hubNS, scheduler)

  protected def this(
      stateClass: Class[S],
      store: SnapshotStore[ID, SS],
      hub: MessageHub[(ID, delta.util.SnapshotUpdate[S])],
      hubNS: MessageHub.Namespace) =
    this(stateClass, store, delta.util.ReadModel.DefaultReadTimeout, hub, hubNS, Threads.DefaultScheduler)

  protected def this(
      stateClass: Class[S],
      store: SnapshotStore[ID, SS],
      hub: MessageHub[(ID, delta.util.SnapshotUpdate[S])],
      hubNS: MessageHub.Namespace,
      scheduler: ScheduledExecutorService) =
    this(stateClass, store, delta.util.ReadModel.DefaultReadTimeout, hub, hubNS, scheduler)

  /**
   * Subscribe to snapshot updates with an initial snapshot.
   * NOTE: The callback will receiver *either* snapshot or update, never both.
   * In other words, the first callback will be a snapshot and all subsequent
   * callbacks will be updates.
   */
  def subscribe(id: ID, callbackExe: Executor, callback: BiConsumer[Snapshot, SnapshotUpdate]): Future[Subscription] = {
      this.subscribe(id, ExecutionContext.fromExecutor(callbackExe, reportFailure)) {
        case Right(update) => callback.accept(null, update)
        case Left(snapshot) => callback.accept(snapshot, null)
      }
    }

}
