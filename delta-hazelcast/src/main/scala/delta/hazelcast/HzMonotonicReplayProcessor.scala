package delta.hazelcast

import scala.reflect.ClassTag
import com.hazelcast.core.IMap
import scala.concurrent.duration.FiniteDuration
import delta.process._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scuff.concurrent.PartitionedExecutionContext
import scuff.concurrent.Threads

object HzMonotonicReplayProcessor {
  private[this] def read[ID, EVT, S](
      imap: IMap[ID, _ <: EntryState[S, EVT]])(id: ID): Future[Option[delta.Snapshot[S]]] = {
    val callback = CallbackPromise[delta.Snapshot[S], Option[delta.Snapshot[S]]] {
      case null => None
      case s: delta.Snapshot[_] => Some {
        s.asInstanceOf[delta.Snapshot[S]]
      }
    }
    imap.submitToKey(id, EntryStateSnapshotReader, callback)
    callback.future
  }

  import ConcurrentMapStore.State

  private def localReplayStore[ID, EVT: ClassTag, S, U](
      cmap: collection.concurrent.Map[ID, State[S]],
      tickWatermark: Option[Long],
      imap: IMap[ID, _ <: EntryState[S, EVT]]): StreamProcessStore[ID, S, U] = {
    ConcurrentMapStore.asReplayStore(cmap, tickWatermark)(read(imap))
  }
}

abstract class HzMonotonicReplayProcessor[ID, EVT: ClassTag, S >: Null, U](
  tickWatermark: Option[Long],
  persistentState: IMap[ID, _ <: EntryState[S, EVT]],
  finalizeProcessingTimeout: FiniteDuration,
  protected val persistenceContext: ExecutionContext,
  partitionThreads: PartitionedExecutionContext,
  cmap: collection.concurrent.Map[ID, ConcurrentMapStore.State[S]])
extends MonotonicReplayProcessor[ID, EVT, S, U, Unit](
  finalizeProcessingTimeout, HzMonotonicReplayProcessor.localReplayStore(cmap, tickWatermark, persistentState))
with ConcurrentMapReplayPersistence[ID, EVT, S, U, Unit] {

  def this(
      tickWatermark: Option[Long],
      persistentState: IMap[ID, EntryState[S, EVT]],
      finalizeProcessingTimeout: FiniteDuration,
      persistContext: ExecutionContext,
      failureReporter: Throwable => Unit,
      localProcessingThreads: Int = 1.max(Runtime.getRuntime.availableProcessors - 1),
      cmap: collection.concurrent.Map[ID, ConcurrentMapStore.State[S]] =
        new collection.concurrent.TrieMap[ID, ConcurrentMapStore.State[S]]) =
    this(tickWatermark, persistentState, finalizeProcessingTimeout, persistContext,
      PartitionedExecutionContext(
          localProcessingThreads, failureReporter,
          Threads.factory(s"${persistentState.getName}-replay-processor", failureReporter)),
      cmap)

  override type Snapshot = delta.Snapshot[S]

  protected def processContext(id: ID): ExecutionContext = partitionThreads.singleThread(id.##)
  protected def onReplayCompletion(): Future[collection.concurrent.Map[ID, State]] =
    partitionThreads.shutdown().map(_ => cmap)(persistenceContext)

  protected def persistReplayState(snapshots: Iterator[(ID, Snapshot)]): Future[Unit] = {
      implicit def ec = persistenceContext
    val updater = EntryStateUpdater[ID, EVT, S](persistentState) _
    val persisted: Iterator[Future[Unit]] = snapshots.flatMap {
      case (id, snapshot) => updater(id, snapshot) :: Nil
    }
    Future.sequence(persisted).map(_ => ())
  }

}
