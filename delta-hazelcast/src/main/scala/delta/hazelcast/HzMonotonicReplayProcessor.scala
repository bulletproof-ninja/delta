package delta.hazelcast

import delta.util.MonotonicReplayProcessor
import scala.reflect.ClassTag
import com.hazelcast.core.IMap
import scala.concurrent.duration.FiniteDuration
import delta.util.StreamProcessStore
import delta.util.ConcurrentMapStore
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scuff.concurrent.PartitionedExecutionContext
import scuff.concurrent.Threads
import delta.util.ConcurrentMapReplayProcessing

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
      imap: IMap[ID, EntryState[S, EVT]]): StreamProcessStore[ID, S] = {
    new ConcurrentMapStore(cmap, None)(read(imap))
  }
}

abstract class HzMonotonicReplayProcessor[ID, EVT: ClassTag, S >: Null](
    imap: IMap[ID, EntryState[S, EVT]],
    whenDoneCompletionTimeout: FiniteDuration,
    protected val whenDoneContext: ExecutionContext,
    partitionThreads: PartitionedExecutionContext,
    cmap: collection.concurrent.Map[ID, delta.Snapshot[S]])
  extends MonotonicReplayProcessor[ID, EVT, S, Unit](
    whenDoneCompletionTimeout, HzMonotonicReplayProcessor.makeStore(cmap, imap))
  with ConcurrentMapReplayProcessing[ID, EVT, S, Unit] {

  def this(
      imap: IMap[ID, EntryState[S, EVT]],
      whenDoneCompletionTimeout: FiniteDuration,
      whenDoneContext: ExecutionContext,
      failureReporter: Throwable => Unit,
      processingThreads: Int = 1.max(Runtime.getRuntime.availableProcessors - 1),
      cmap: collection.concurrent.Map[ID, delta.Snapshot[S]] = new collection.concurrent.TrieMap[ID, delta.Snapshot[S]]) =
    this(imap, whenDoneCompletionTimeout, whenDoneContext,
      PartitionedExecutionContext(processingThreads, failureReporter, Threads.factory(s"${imap.getName}-replay-processor")),
      cmap)

  protected def processingContext(id: ID): ExecutionContext = partitionThreads.singleThread(id.##)
  protected def onReplayCompletion(): Future[collection.concurrent.Map[ID, Snapshot]] =
    partitionThreads.shutdown().map(_ => cmap)(whenDoneContext)

  protected def persist(snapshots: collection.concurrent.Map[ID, Snapshot]): Future[Unit] = {
      implicit def ec = whenDoneContext
    val updater = EntryStateUpdater[ID, EVT, S](imap) _
    val persisted = snapshots.collect {
      case (id, snapshot) => updater(id, snapshot)
    }
    Future.sequence(persisted).map(_ => ())
  }

}
