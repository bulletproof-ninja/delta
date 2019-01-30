package delta.util

import scala.concurrent._

import scuff.StreamConsumer
import scuff.concurrent._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.concurrent.duration._
import java.util.concurrent.ScheduledExecutorService
import delta.Transaction

/**
 * Default implementation of [[delta.util.EventSourceConsumer]].
 */
abstract class MonotonicEventSourceConsumer[ID, EVT: ClassTag, S >: Null](
    store: StreamProcessStore[ID, S],
    scheduler: ScheduledExecutorService)
  extends EventSourceConsumer[ID, EVT]
  with TransactionProcessor[ID, EVT, S] {

  override protected type TXN = Transaction[ID, _ >: EVT]
  protected type SnapshotUpdate = delta.util.SnapshotUpdate[S]
  protected type Snapshot = delta.Snapshot[S]
  protected def tickWatermark: Option[Long] = store.tickWatermark

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

  private class ReplayProcessor
    extends DefaultMonotonicReplayProcessor[ID, EVT, S](store, replayProcessorCompletionTimeout, ExecutionContext.fromExecutorService(scheduler, reportFailure), replayProcessorWriteBatchSize, newPartitionedExecutionContext, newReplayMap) {
    protected def process(tx: TXN, state: Option[S]): S = ???
    override protected def processAsync(tx: TXN, state: Option[S]): Future[S] =
      MonotonicEventSourceConsumer.this.processAsync(tx, state).asInstanceOf[Future[S]]
  }

  private class LiveProcessor(es: ES)
    extends DefaultMonotonicProcessor[ID, EVT, S](es, store, replayMissingRevisionsDelay, scheduler, newPartitionedExecutionContext) {
    protected def onSnapshotUpdate(id: ID, update: SnapshotUpdate) = 
      MonotonicEventSourceConsumer.this.onSnapshotUpdate(id, update)
    protected def process(tx: TXN, state: Option[S]): S = ???
    override protected def processAsync(tx: TXN, state: Option[S]): Future[S] =
      MonotonicEventSourceConsumer.this.processAsync(tx, state)

  }

  protected def replayProcessor(es: ES): StreamConsumer[TXN, Future[ReplayResult]] =
    new ReplayProcessor

  protected def liveProcessor(es: ES, replayResult: Option[ReplayResult]): MonotonicProcessor[ID, EVT, S] =
    new LiveProcessor(es)

}
