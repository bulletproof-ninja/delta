package delta.hazelcast

import com.hazelcast.core.IMap

import delta.Tick
import delta.process._

import scala.concurrent._
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
  protected val replayConfig: ReplayProcessConfig,
  partitionThreads: PartitionedExecutionContext,
  cmap: collection.concurrent.Map[ID, ConcurrentMapStore.State[S]])(
  implicit
  protected val executionContext: ExecutionContext)
extends MonotonicReplayProcessor[ID, EVT, S, U]
with ConcurrentMapReplayPersistence[ID, EVT, S, U] {

  def this(
      tickWatermark: Option[Tick],
      persistentState: IMap[ID, EntryState[S, EVT]],
      replayConfig: ReplayProcessConfig,
      failureReporter: Throwable => Unit,
      localThreadCount: Int = 1.max(Runtime.getRuntime.availableProcessors - 1),
      cmap: collection.concurrent.Map[ID, ConcurrentMapStore.State[S]] =
        new collection.concurrent.TrieMap[ID, ConcurrentMapStore.State[S]])(
      implicit
      ec: ExecutionContext) =
    this(
      tickWatermark, persistentState, replayConfig,
      PartitionedExecutionContext(
          localThreadCount, failureReporter,
          Threads.factory(s"${persistentState.getName}-replay-processor", failureReporter)),
      cmap)

  require(
    !replayConfig.writeTickOrdered,
    s"Cannot enforce writing in tick order. Set ${classOf[ReplayProcessConfig].getSimpleName}(writeTickOrdered=false)")

  override type Snapshot = delta.Snapshot[S]

  protected val processStore =
    HzMonotonicReplayProcessor.localReplayStore(cmap, tickWatermark, persistentState)

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
