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
      imap: IMap[ID, EntryState[S, EVT]])(id: ID): Future[Option[delta.Snapshot[S]]] = {
    val callback = CallbackPromise[delta.Snapshot[S], Option[delta.Snapshot[S]]] {
      case null => None
      case s: delta.Snapshot[_] => Some {
        s.asInstanceOf[delta.Snapshot[S]]
      }
    }
    imap.submitToKey(id, EntryStateSnapshotReader, callback)
    callback.future
  }    
  
  import ConcurrentMapStore.Value

  private def makeStore[ID, EVT: ClassTag, S](
      cmap: collection.concurrent.Map[ID, Value[S]],
      tickWatermark: Option[Long],
      imap: IMap[ID, EntryState[S, EVT]]): StreamProcessStore[ID, S] = {
    new ConcurrentMapStore(cmap, tickWatermark)(read(imap))
  }
}

abstract class HzMonotonicReplayProcessor[ID, EVT: ClassTag, S >: Null](
    tickWatermark: Option[Long],
    imap: IMap[ID, EntryState[S, EVT]],
    finishProcessingTimeout: FiniteDuration,
    protected val persistenceContext: ExecutionContext,
    partitionThreads: PartitionedExecutionContext,
    cmap: collection.concurrent.Map[ID, ConcurrentMapStore.Value[S]])
  extends MonotonicReplayProcessor[ID, EVT, S, Unit](
    finishProcessingTimeout, HzMonotonicReplayProcessor.makeStore(cmap, tickWatermark, imap))
  with ConcurrentMapReplayPersistence[ID, EVT, S, Unit] {

  def this(
      tickWatermark: Option[Long],
      imap: IMap[ID, EntryState[S, EVT]],
      finishProcessingTimeout: FiniteDuration,
      persistContext: ExecutionContext,
      failureReporter: Throwable => Unit,
      processingThreads: Int = 1.max(Runtime.getRuntime.availableProcessors - 1),
      cmap: collection.concurrent.Map[ID, ConcurrentMapStore.Value[S]] = 
        new collection.concurrent.TrieMap[ID, ConcurrentMapStore.Value[S]]) =
    this(tickWatermark, imap, finishProcessingTimeout, persistContext,
      PartitionedExecutionContext(processingThreads, failureReporter, Threads.factory(s"${imap.getName}-replay-processor", failureReporter)),
      cmap)

  protected def processContext(id: ID): ExecutionContext = partitionThreads.singleThread(id.##)
  protected def onReplayCompletion(): Future[collection.concurrent.Map[ID, Value]] =
    partitionThreads.shutdown().map(_ => cmap)(persistenceContext)

  protected def persistReplayState(snapshots: Iterator[(ID, Snapshot)]): Future[Unit] = {
      implicit def ec = persistenceContext
    val updater = EntryStateUpdater[ID, EVT, S](imap) _
    val persisted: Iterator[Future[Unit]] = snapshots.flatMap {
      case (id, snapshot) => updater(id, snapshot) :: Nil
    }
    Future.sequence(persisted).map(_ => ())
  }

}
