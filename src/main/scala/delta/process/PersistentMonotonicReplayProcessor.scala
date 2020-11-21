package delta.process

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
  protected val replayConfig: ReplayProcessConfig,
  partitionThreads: PartitionedExecutionContext,
  cmap: CMap[ID, ConcurrentMapStore.State[S]])
extends MonotonicReplayProcessor[ID, EVT, S, U]
with ConcurrentMapReplayPersistence[ID, EVT, S, U] {

  def this(
      persistentStore: StreamProcessStore[ID, S, U],
      replayConfig: ReplayProcessConfig,
      whenDoneContext: ExecutionContext,
      failureReporter: Throwable => Unit,
      processingThreads: Int = 1.max(Runtime.getRuntime.availableProcessors - 1),
      cmap: CMap[ID, ConcurrentMapStore.State[S]] = new TrieMap[ID, ConcurrentMapStore.State[S]]) =
    this(persistentStore, replayConfig,
      PartitionedExecutionContext(
        processingThreads, failureReporter, Threads.factory(s"default-replay-processor", failureReporter)),
      cmap)

  override type Snapshot = delta.Snapshot[S]

  protected val processStore = ConcurrentMapStore.asReplayStore(cmap, persistentStore)

  protected def processContext(id: ID) = partitionThreads.singleThread(id.##)

  protected def onReplayCompletion(): Future[collection.concurrent.Map[ID, State]] =
    partitionThreads.shutdown().map(_ => cmap)(executionContext)

  protected final def persistReplayState(snapshots: Iterator[(ID, Snapshot)]) =
    if (replayConfig.writeTickOrdered) persistOrdered(replayConfig.writeBatchSize, snapshots)
    else persistParallel(replayConfig.writeBatchSize, snapshots)

  private def persistOrdered(batchSize: Int, snapshots: Iterator[(ID, Snapshot)]): Future[Unit] =
    if (batchSize > 1) {
      snapshots.grouped(batchSize).foldLeft(Future.unit) {
        case (prevWrite, batch) =>
          prevWrite.flatMap { _ =>
            persistentStore writeBatch batch.toMap
          }
      }
    } else {
      snapshots.foldLeft(Future.unit) {
        case (prevWrite, snapshot) =>
          prevWrite.flatMap { _ =>
            persistentStore write snapshot
          }
      }
    }

  private def persistParallel(batchSize: Int, snapshots: Iterator[(ID, Snapshot)]): Future[Unit] = {
    val writes: Iterator[Future[Unit]] =
      if (batchSize > 1) {
        snapshots.grouped(batchSize).map { batch =>
          persistentStore writeBatch batch.toMap
        }
      } else {
        snapshots.map(persistentStore.write)
      }

    Future.sequence(writes).map(_ => ())
  }

}
