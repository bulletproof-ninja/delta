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
  private def makeStore[ID, EVT: ClassTag, S](
      cmap: collection.concurrent.Map[ID, delta.Snapshot[S]],
      tickWatermark: Option[Long],
      imap: IMap[ID, EntryState[S, EVT]]): StreamProcessStore[ID, S] = {
    new ConcurrentMapStore(cmap, tickWatermark)(read(imap))
  }
}

abstract class HzMonotonicReplayProcessor[ID, EVT: ClassTag, S >: Null](
    tickWatermark: Option[Long],
    imap: IMap[ID, EntryState[S, EVT]],
    finishProcessingTimeout: FiniteDuration,
    protected val persistContext: ExecutionContext,
    partitionThreads: PartitionedExecutionContext,
    cmap: collection.concurrent.Map[ID, delta.Snapshot[S]])
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
      cmap: collection.concurrent.Map[ID, delta.Snapshot[S]] = new collection.concurrent.TrieMap[ID, delta.Snapshot[S]]) =
    this(tickWatermark, imap, finishProcessingTimeout, persistContext,
      PartitionedExecutionContext(processingThreads, failureReporter, Threads.factory(s"${imap.getName}-replay-processor", failureReporter)),
      cmap)

  protected def processContext(id: ID): ExecutionContext = partitionThreads.singleThread(id.##)
  protected def onReplayCompletion(): Future[collection.concurrent.Map[ID, Snapshot]] =
    partitionThreads.shutdown().map(_ => cmap)(persistContext)

  protected def persist(snapshots: collection.concurrent.Map[ID, Snapshot]): Future[Unit] = {
      implicit def ec = persistContext
    val updater = EntryStateUpdater[ID, EVT, S](imap) _
    val persisted: Iterable[Future[Unit]] = snapshots.map {
      case (id, snapshot) => updater(id, snapshot)
    }
    Future.sequence(persisted).map(_ => ())
  }

}
