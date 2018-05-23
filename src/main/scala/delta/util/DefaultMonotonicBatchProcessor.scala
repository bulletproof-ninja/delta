package delta.util

import scala.concurrent.duration.FiniteDuration
import scuff.concurrent.PartitionedExecutionContext
import scala.concurrent.Future
import scala.reflect.ClassTag
import scuff.concurrent.Threads

abstract class DefaultMonotonicBatchProcessor[ID, EVT: ClassTag, S >: Null](
    mapStore: collection.concurrent.Map[ID, delta.Snapshot[S]],
    store: StreamProcessStore[ID, S],
    batchProcessorCompletionTimeout: FiniteDuration,
    batchProcessorWriteBatchSize: Int,
    partitionThreads: PartitionedExecutionContext)
  extends MonotonicBatchProcessor[ID, EVT, S, Unit](batchProcessorCompletionTimeout, new ConcurrentMapStore(mapStore)(store.read)) {

  protected def executionContext(id: ID) = partitionThreads.singleThread(id.hashCode)

  def whenDone(): Future[Unit] = {
    implicit def ec = Threads.PiggyBack
    partitionThreads.shutdown()
      .flatMap { _ =>
        if (unfinishedStreams.nonEmpty) {
          val ids = unfinishedStreams.mkString(compat.Platform.EOL, ", ", "")
          throw new IllegalStateException(s"Incomplete stream processing for ids:$ids")
        }
        val iter = mapStore.iterator
        val writes = collection.mutable.Buffer[Future[Unit]]()
        while (iter.hasNext) {
          if (batchProcessorWriteBatchSize > 1) {
            val batch = iter.take(batchProcessorWriteBatchSize).toMap
            writes += processStore.writeBatch(batch)
          } else if (batchProcessorWriteBatchSize == 1) {
            val (id, snapshot) = iter.next
            writes += processStore.write(id, snapshot)
          } else { // batchProcessorWriteBatchSize <= 0
            val batch = iter.toMap
            writes += processStore.writeBatch(batch)
          }
        }
        Future.sequence(writes)
      } map { _ =>
        mapStore.clear()
      }
  }
}
