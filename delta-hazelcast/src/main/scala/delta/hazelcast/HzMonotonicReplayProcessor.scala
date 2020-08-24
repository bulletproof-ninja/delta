package delta.hazelcast

import com.hazelcast.core.IMap

import delta.Tick
import delta.process._

import scala.concurrent._, duration.FiniteDuration
import scala.reflect.ClassTag

import scuff.concurrent.{ PartitionedExecutionContext, Threads }

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
      tickWatermark: Option[Tick],
      imap: IMap[ID, _ <: EntryState[S, EVT]]): StreamProcessStore[ID, S, U] = {
    ConcurrentMapStore.asReplayStore(cmap, imap.getName, tickWatermark)(read(imap))
  }
}

abstract class HzMonotonicReplayProcessor[ID, EVT: ClassTag, S >: Null, U](
  tickWatermark: Option[Tick],
  persistentState: IMap[ID, _ <: EntryState[S, EVT]],
  finalizeProcessingTimeout: FiniteDuration,
  partitionThreads: PartitionedExecutionContext,
  cmap: collection.concurrent.Map[ID, ConcurrentMapStore.State[S]])(
  implicit
  ec: ExecutionContext)
extends MonotonicReplayProcessor[ID, EVT, S, U](
  finalizeProcessingTimeout, HzMonotonicReplayProcessor.localReplayStore(cmap, tickWatermark, persistentState))
with ConcurrentMapReplayPersistence[ID, EVT, S, U] {

  def this(
      tickWatermark: Option[Tick],
      persistentState: IMap[ID, EntryState[S, EVT]],
      finalizeProcessingTimeout: FiniteDuration,
      failureReporter: Throwable => Unit,
      localThreadCount: Int = 1.max(Runtime.getRuntime.availableProcessors - 1),
      cmap: collection.concurrent.Map[ID, ConcurrentMapStore.State[S]] =
        new collection.concurrent.TrieMap[ID, ConcurrentMapStore.State[S]])(
      implicit
      ec: ExecutionContext) =
    this(
      tickWatermark, persistentState, finalizeProcessingTimeout,
      PartitionedExecutionContext(
          localThreadCount, failureReporter,
          Threads.factory(s"${persistentState.getName}-replay-processor", failureReporter)),
      cmap)

  override type Snapshot = delta.Snapshot[S]

  protected def processContext(id: ID): ExecutionContext = partitionThreads.singleThread(id.##)
  protected def onReplayCompletion(): Future[collection.concurrent.Map[ID, State]] =
    partitionThreads.shutdown().map(_ => cmap)

  protected def persistReplayState(snapshots: Iterator[(ID, Snapshot)]): Future[Unit] = {
    val updater = EntryStateUpdater[ID, EVT, S](persistentState) _
    val persisted: Iterator[Future[Unit]] = snapshots.flatMap {
      case (id, snapshot) => updater(id, snapshot) :: Nil
    }
    Future.sequence(persisted).map(_ => ())
  }

}
