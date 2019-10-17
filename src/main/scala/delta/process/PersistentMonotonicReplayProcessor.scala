package delta.process

import scala.concurrent.duration.FiniteDuration
import scuff.concurrent.PartitionedExecutionContext
import scala.concurrent.Future
import scala.reflect.ClassTag
import scuff.concurrent.Threads
import scala.concurrent.ExecutionContext

abstract class PersistentMonotonicReplayProcessor[ID, EVT: ClassTag, S >: Null](
    persistentStore: StreamProcessStore[ID, S],
    whenDoneCompletionTimeout: FiniteDuration,
    protected val persistenceContext: ExecutionContext,
    writeBatchSize: Int,
    partitionThreads: PartitionedExecutionContext,
    cmap: collection.concurrent.Map[ID, ConcurrentMapStore.Value[S]])
  extends MonotonicReplayProcessor[ID, EVT, S, Unit](
      whenDoneCompletionTimeout,
      ConcurrentMapStore(cmap, persistentStore.tickWatermark)(persistentStore.read))
  with ConcurrentMapReplayPersistence[ID, EVT, S, Unit] {

  def this(
      store: StreamProcessStore[ID, S],
      whenDoneCompletionTimeout: FiniteDuration,
      whenDoneContext: ExecutionContext,
      writeBatchSize: Int,
      failureReporter: Throwable => Unit,
      processingThreads: Int = 1.max(Runtime.getRuntime.availableProcessors - 1),
      cmap: collection.concurrent.Map[ID, ConcurrentMapStore.Value[S]] = 
        new collection.concurrent.TrieMap[ID, ConcurrentMapStore.Value[S]]) =
    this(store, whenDoneCompletionTimeout, whenDoneContext, writeBatchSize,
      PartitionedExecutionContext(processingThreads, failureReporter, Threads.factory(s"default-replay-processor", failureReporter)),
      cmap)
      
  protected def processContext(id: ID) = partitionThreads.singleThread(id.##)

  protected def onReplayCompletion(): Future[collection.concurrent.Map[ID, Value]] =
    partitionThreads.shutdown().map(_ => cmap)(persistenceContext)

  protected def persistReplayState(snapshots: Iterator[(ID, Snapshot)]): Future[Unit] = {
    val writes = collection.mutable.Buffer[Future[Unit]]()
    while (snapshots.hasNext) {
      if (writeBatchSize > 1) {
        val batch = snapshots.take(writeBatchSize).toMap
        writes += persistentStore.writeBatch(batch)
      } else if (writeBatchSize == 1) {
        val (id, snapshot) = snapshots.next
        writes += persistentStore.write(id, snapshot)
      } else { // replayProcessorWriteReplaySize <= 0
        val batch = snapshots.toMap
        writes += persistentStore.writeBatch(batch)
      }
    }
      implicit def ec = persistenceContext
    Future.sequence(writes).map(_ => ())
  }

}
