package delta.process

import scala.concurrent._
import scuff.StreamConsumer
import scuff.concurrent._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.concurrent.duration._
import java.util.concurrent.ScheduledExecutorService
import delta.Transaction

/**
 * Default monotonic implementation of [[delta.process.EventSourceConsumer]].
 * Can be started and stopped on demand, without loss of
 * transactions.
 * NOTE: Since replay processing is generally
 * not done in tick order, a tick received
 * through incomplete replay processing
 * is NOT reliable as a high-water mark. In other words,
 * replay processing should not use a [[delta.process.StreamProcessStore]]
 * until replay processing has completed.
 * Any tick received in live processing can be considered a
 * high-water mark (subject to tick skew).
 */
abstract class PersistentMonotonicConsumer[ID, EVT: ClassTag, S >: Null](
    protected val processStore: StreamProcessStore[ID, S],
    scheduler: ScheduledExecutorService)
  extends EventSourceConsumer[ID, EVT]
  with TransactionProcessor[ID, EVT, S] {

  override protected type TXN = Transaction[ID, _ >: EVT]
  protected type SnapshotUpdate = delta.process.SnapshotUpdate[S]
  protected type Snapshot = delta.Snapshot[S]
  protected def tickWatermark: Option[Long] = processStore.tickWatermark

  protected def reportFailure(th: Throwable): Unit

  /**
    * Callback on snapshot updates from live processing.
    * @param id The stream id
    * @param update The snapshot update
    */
  protected def onSnapshotUpdate(id: ID, update: SnapshotUpdate): Unit

  /** Partitions on stream id. Defaults to `availableProcessors - 1`. */
  protected def newPartitionedExecutionContext: PartitionedExecutionContext = {
    val numThreads = 1 max (Runtime.getRuntime.availableProcessors - 1)
    PartitionedExecutionContext(numThreads, failureReporter = reportFailure)
  }

  /**
    * Instantiate new concurrent map used to hold state during
    * replay processing. This can be overridden to provide a
    * different implementation that e.g. stores to local disk,
    * if data set is too large for in-memory handling.
    */
  protected def newReplayMap: collection.concurrent.Map[ID, Snapshot] =
    new java.util.concurrent.ConcurrentHashMap[ID, Snapshot].asScala

  protected type ReplayResult = Any

  /**
    * Time allowed for replay processor to finish remaining
    * transactions once replay has finished.
    */
  protected def replayProcessorCompletionTimeout: FiniteDuration = 11.seconds

  /**
   * Batch size when writing result of replay.
   */
  protected def replayProcessorWriteBatchSize: Int = 1000

  /**
    * Time delay before replaying missing revisions.
    * This allows some margin for delayed out-of-order transactions.
    */
  protected def replayMissingRevisionsDelay: FiniteDuration = 1111.milliseconds

  protected class ReplayProcessor
    extends DefaultMonotonicReplayProcessor[ID, EVT, S](processStore, replayProcessorCompletionTimeout, ExecutionContext.fromExecutorService(scheduler, reportFailure), replayProcessorWriteBatchSize, newPartitionedExecutionContext, newReplayMap) {
    protected def process(tx: TXN, state: Option[S]): S = ???
    override protected def processAsync(tx: TXN, state: Option[S]): Future[S] =
      PersistentMonotonicConsumer.this.processAsync(tx, state).asInstanceOf[Future[S]]
  }

  protected class LiveProcessor(es: ES)
    extends DefaultMonotonicProcessor[ID, EVT, S](es, processStore, replayMissingRevisionsDelay, scheduler, newPartitionedExecutionContext) {
    protected def onSnapshotUpdate(id: ID, update: SnapshotUpdate) =
      PersistentMonotonicConsumer.this.onSnapshotUpdate(id, update)
    protected def process(tx: TXN, state: Option[S]): S = ???
    override protected def processAsync(tx: TXN, state: Option[S]): Future[S] =
      PersistentMonotonicConsumer.this.processAsync(tx, state)

  }

  protected def replayProcessor(es: ES): StreamConsumer[TXN, Future[ReplayResult]] =
    new ReplayProcessor

//  protected def liveProcessor(es: ES, replayResult: Option[ReplayResult]): MonotonicProcessor[ID, EVT, S] =
  protected def liveProcessor(es: ES, replayResult: Option[ReplayResult]): TXN => Any =
    new LiveProcessor(es)

}
