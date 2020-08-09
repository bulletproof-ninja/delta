package delta.process

import scala.concurrent.duration.FiniteDuration
import scuff.concurrent.PartitionedExecutionContext
import scala.concurrent._
import scala.reflect.ClassTag
import scuff.concurrent.Threads
import scala.collection.concurrent.{ Map => CMap, _ }

/**
  * Monotonic replay processor that uses a `concurrent.Map`
  * to hold temporary replay state, eventually persisting
  * to the [[delta.process.StreamProcessStore]].
  * Processing is done on threads partitioned by `ID`,
  * to ensure monotonic ordering and ''potentially'' better
  * L2+ cache utilization.
  *
  * @param persistentStore The persistent store
  * @param postReplayTimeout
  * @param writeBatchSize
  * @param partitionThreads
  * @param cmap
  */
abstract class PersistentMonotonicReplayProcessor[ID, EVT: ClassTag, S >: Null, U](
  persistentStore: StreamProcessStore[ID, S, U],
  postReplayTimeout: FiniteDuration,
  writeBatchSize: Int,
  partitionThreads: PartitionedExecutionContext,
  cmap: CMap[ID, ConcurrentMapStore.State[S]])(
  implicit
  ec: ExecutionContext)
extends MonotonicReplayProcessor[ID, EVT, S, U, Unit](
  postReplayTimeout,
  ConcurrentMapStore.asReplayStore(cmap, persistentStore))
with ConcurrentMapReplayPersistence[ID, EVT, S, U, Unit] {

  def this(
      persistentStore: StreamProcessStore[ID, S, U],
      postReplayTimeout: FiniteDuration,
      whenDoneContext: ExecutionContext,
      writeBatchSize: Int,
      failureReporter: Throwable => Unit,
      processingThreads: Int = 1.max(Runtime.getRuntime.availableProcessors - 1),
      cmap: CMap[ID, ConcurrentMapStore.State[S]] = new TrieMap[ID, ConcurrentMapStore.State[S]])(
      implicit
      ec: ExecutionContext) =
    this(persistentStore, postReplayTimeout, writeBatchSize,
      PartitionedExecutionContext(
        processingThreads, failureReporter, Threads.factory(s"default-replay-processor", failureReporter)),
      cmap)

  override type Snapshot = delta.Snapshot[S]

  protected def processContext(id: ID) = partitionThreads.singleThread(id.##)

  protected def onReplayCompletion(): Future[collection.concurrent.Map[ID, State]] =
    partitionThreads.shutdown().map(_ => cmap)(executionContext)

  protected final def persistReplayState(snapshots: Iterator[(ID, Snapshot)]) = {

    val writes: Iterator[Future[Unit]] =
      if (writeBatchSize > 1) {
        snapshots.grouped(writeBatchSize).map { batch =>
          persistentStore writeBatch batch.toMap
        }
      } else {
        snapshots.map(persistentStore.write)
      }

    Future.sequence(writes).map(_ => ())
  }

}
