package delta.util

import scala.concurrent._

import scuff.StreamConsumer
import scuff.concurrent._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.concurrent.duration._
import java.util.concurrent.ScheduledExecutorService

abstract class DefaultEventSourceConsumer[ID, EVT: ClassTag, S >: Null](
    protected val store: StreamProcessStore[ID, S],
    protected val scheduler: ScheduledExecutorService,
    batchProcessorWriteBatchSize: Int)
  extends EventSourceConsumer[ID, EVT]
  with TransactionProcessor[ID, EVT, S] {

  protected type Snapshot = delta.Snapshot[S]
  protected def tickWatermark: Option[Long] = store.tickWatermark

  require(batchProcessorWriteBatchSize > 0, s"Write batch size must be positive, was $batchProcessorWriteBatchSize")

  protected def reportFailure(th: Throwable): Unit
  /** Event source selector. */
  protected def selector(es: ES): es.CompleteSelector

  /**
    * Realtime callback on snapshot updates.
    * @param id Snapshot (stream) id
    * @param snapshot The snapshot content, revision, and tick
    * @param contentUpdated `true` if snapshot content was updated, `false` if just revision and/or tick
    */
  protected def onSnapshotUpdate(id: ID, snapshot: Snapshot, contentUpdated: Boolean): Unit

  /** Partitions on stream id. Defaults to `availableProcessors - 1`. */
  protected def newPartitionedExecutionContext: PartitionedExecutionContext = {
    val numThreads = 1 max (Runtime.getRuntime.availableProcessors - 1)
    PartitionedExecutionContext(numThreads, failureReporter = reportFailure)
  }

  /**
    * Instantiate new concurrent map used to hold state during
    * batch processing. This can be overridden to provide a
    * different implementation that e.g. stores to local disk,
    * if data set is too large for in-memory handling.
    */
  protected def newBatchMap[K, V]: collection.concurrent.Map[K, V] =
    new java.util.concurrent.ConcurrentHashMap[K, V].asScala

  protected type BatchResult = Any

  /**
    * Time allowed for batch processor to finish remaining
    * transactions once replay has finished.
    */
  protected def batchProcessorCompletionTimeout: FiniteDuration = 11.seconds

  private class BatchProcessor
    extends DefaultMonotonicBatchProcessor[ID, EVT, S](newBatchMap[ID, Snapshot], store, batchProcessorCompletionTimeout, batchProcessorWriteBatchSize, newPartitionedExecutionContext) {
    protected def process(tx: TXN, state: Option[S]): S = ???
    override protected def processAsync(tx: TXN, state: Option[S]): Future[S] =
      DefaultEventSourceConsumer.this.processAsync(tx, state)
  }

  private class RealtimeProcessor(es: ES)
    extends DefaultMonotonicProcessor[ID, EVT, S](es, store, newPartitionedExecutionContext, replayMissingRevisionsDelay, scheduler) {
    protected def onUpdate(id: ID, update: Update) = onSnapshotUpdate(id, update.snapshot, update.contentUpdated)
    protected def process(tx: TXN, state: Option[S]): S = ???
    override protected def processAsync(tx: TXN, state: Option[S]): Future[S] =
      DefaultEventSourceConsumer.this.processAsync(tx, state)

  }

  protected def batchProcessor(es: ES): StreamConsumer[TXN, Future[BatchResult]] = this match {
    case js: JoinState[ID, EVT, S] =>
      new BatchProcessor with JoinStateProcessor[ID, EVT, S, S] {
        def preprocess(streamId: ID, streamRevision: Int, tick: Long, evt: EVT, metadata: Map[String, String]): Map[ID, Processor] = ???
        override def preprocess(txn: TXN)(implicit ev: ClassTag[EVT]): Map[ID, Processor] =
          js.preprocess(txn)(ev)
      }
    case _ =>
      new BatchProcessor
  }

  protected def realtimeProcessor(es: ES, batchResult: Option[BatchResult]): MonotonicProcessor[ID, EVT, S] =
    this match {
      case js: JoinState[ID, EVT, S] =>
        new RealtimeProcessor(es) with JoinStateProcessor[ID, EVT, S, S] {
          def preprocess(streamId: ID, streamRevision: Int, tick: Long, evt: EVT, metadata: Map[String, String]): Map[ID, Processor] = ???
          override def preprocess(txn: TXN)(implicit ev: ClassTag[EVT]): Map[ID, Processor] =
            js.preprocess(txn)(ev)
        }
      case _ =>
        new RealtimeProcessor(es)
    }

  protected def replayMissingRevisionsDelay = 1111.milliseconds

}
