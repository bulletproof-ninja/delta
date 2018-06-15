package delta.hazelcast

import delta.util.MonotonicBatchProcessor
import scala.reflect.ClassTag
import com.hazelcast.core.IMap
import scala.concurrent.duration.FiniteDuration
import delta.util.StreamProcessStore
import delta.util.ConcurrentMapStore
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scuff.concurrent.PartitionedExecutionContext
import scuff.concurrent.Threads
import scuff.Codec
import delta.util.ConcurrentMapBatchProcessing

object HzMonotonicBatchProcessor {
  private[this] def read[ID, EVT, WS, RS](
      imap: IMap[ID, EntryState[RS, EVT]],
      conv: RS => WS)(id: ID): Future[Option[delta.Snapshot[WS]]] = {
    val callback = CallbackPromise[delta.Snapshot[RS], Option[delta.Snapshot[WS]]] {
      case null => None
      case s: delta.Snapshot[_] => Some {
        s.asInstanceOf[delta.Snapshot[RS]].map(conv)
      }
    }
    imap.submitToKey(id, EntryStateSnapshotReader, callback)
    callback.future
  }
  private def makeStore[ID, EVT: ClassTag, WS, RS](
      cmap: collection.concurrent.Map[ID, delta.Snapshot[WS]],
      imap: IMap[ID, EntryState[RS, EVT]],
      stateCodec: Codec[RS, WS]): StreamProcessStore[ID, WS] = {
    new ConcurrentMapStore(cmap, None)(read(imap, stateCodec.encode))
  }
}

abstract class HzMonotonicBatchProcessor[ID, EVT: ClassTag, WS >: Null, RS](
    imap: IMap[ID, EntryState[RS, EVT]],
    stateCodec: Codec[RS, WS],
    whenDoneCompletionTimeout: FiniteDuration,
    protected val whenDoneContext: ExecutionContext,
    partitionThreads: PartitionedExecutionContext,
    cmap: collection.concurrent.Map[ID, delta.Snapshot[WS]])
  extends MonotonicBatchProcessor[ID, EVT, WS, Unit](
    whenDoneCompletionTimeout, HzMonotonicBatchProcessor.makeStore(cmap, imap, stateCodec))
  with ConcurrentMapBatchProcessing[ID, EVT, WS, Unit] {

  def this(
      imap: IMap[ID, EntryState[RS, EVT]],
      stateCodec: Codec[RS, WS],
      whenDoneCompletionTimeout: FiniteDuration,
      whenDoneContext: ExecutionContext,
      failureReporter: Throwable => Unit,
      processingThreads: Int = 1.max(Runtime.getRuntime.availableProcessors - 1),
      cmap: collection.concurrent.Map[ID, delta.Snapshot[WS]] = new collection.concurrent.TrieMap[ID, delta.Snapshot[WS]]) =
    this(imap, stateCodec, whenDoneCompletionTimeout, whenDoneContext,
      PartitionedExecutionContext(processingThreads, failureReporter, Threads.factory(s"${imap.getName}-batch-processor")),
      cmap)

  protected def processingContext(id: ID): ExecutionContext = partitionThreads.singleThread(id.##)
  protected def onBatchStreamCompletion(): Future[collection.concurrent.Map[ID, Snapshot]] =
    partitionThreads.shutdown().map(_ => cmap)(whenDoneContext)

  protected def persist(snapshots: collection.concurrent.Map[ID, Snapshot]): Future[Unit] = {
      implicit def ec = whenDoneContext
    val updater = EntryStateUpdater[ID, EVT, RS](imap) _
    val persisted = snapshots.collect {
      case (id, snapshot) => updater(id, snapshot.map(stateCodec.decode))
    }
    Future.sequence(persisted).map(_ => ())
  }

}
