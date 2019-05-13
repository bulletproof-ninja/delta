package delta.process

import scala.concurrent.duration.FiniteDuration
import scuff.concurrent.PartitionedExecutionContext
import scala.concurrent.Future
import scala.reflect.ClassTag
import scuff.concurrent.Threads
import scala.concurrent.ExecutionContext

abstract class DefaultMonotonicReplayProcessor[ID, EVT: ClassTag, S >: Null](
    store: StreamProcessStore[ID, S],
    whenDoneCompletionTimeout: FiniteDuration,
    protected val persistContext: ExecutionContext,
    writeBatchSize: Int,
    partitionThreads: PartitionedExecutionContext,
    cmap: collection.concurrent.Map[ID, delta.Snapshot[S]])
  extends MonotonicReplayProcessor[ID, EVT, S, Unit](
      whenDoneCompletionTimeout,
      new ConcurrentMapStore(cmap, store.tickWatermark)(store.read))
  with ConcurrentMapReplayPersistence[ID, EVT, S, Unit] {

  def this(
      store: StreamProcessStore[ID, S],
      whenDoneCompletionTimeout: FiniteDuration,
      whenDoneContext: ExecutionContext,
      writeBatchSize: Int,
      failureReporter: Throwable => Unit,
      processingThreads: Int = 1.max(Runtime.getRuntime.availableProcessors - 1),
      cmap: collection.concurrent.Map[ID, delta.Snapshot[S]] = new collection.concurrent.TrieMap[ID, delta.Snapshot[S]]) =
    this(store, whenDoneCompletionTimeout, whenDoneContext, writeBatchSize,
      PartitionedExecutionContext(processingThreads, failureReporter, Threads.factory(s"default-replay-processor")),
      cmap)

  protected def processContext(id: ID) = partitionThreads.singleThread(id.hashCode)

  protected def onReplayCompletion(): Future[collection.concurrent.Map[ID, Snapshot]] =
    partitionThreads.shutdown().map(_ => cmap)(persistContext)

  protected def persist(snapshots: collection.concurrent.Map[ID, Snapshot]): Future[Unit] = {
    val iter = snapshots.iterator
    val writes = collection.mutable.Buffer[Future[Unit]]()
    while (iter.hasNext) {
      if (writeBatchSize > 1) {
        val batch = iter.take(writeBatchSize).toMap
        writes += processStore.writeBatch(batch)
      } else if (writeBatchSize == 1) {
        val (id, snapshot) = iter.next
        writes += processStore.write(id, snapshot)
      } else { // replayProcessorWriteReplaySize <= 0
        val batch = iter.toMap
        writes += processStore.writeBatch(batch)
      }
    }
      implicit def ec = persistContext
    Future.sequence(writes).map(_ => ())
  }

}
