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
    replayProcessorWriteBatchSize: Int)
  extends EventSourceConsumer[ID, EVT]
  with TransactionProcessor[ID, EVT, S] {

  protected type Snapshot = delta.Snapshot[S]
  protected def tickWatermark: Option[Long] = store.tickWatermark

  require(replayProcessorWriteBatchSize > 0, s"Write batch size must be positive, was $replayProcessorWriteBatchSize")

  protected def reportFailure(th: Throwable): Unit
  /** Event source selector. */
  protected def selector(es: ES): es.CompleteSelector

  /**
    * Callback on snapshot updates from live processing.
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
    * Time delay before replaying missing revisions.
    * This allows some margin for delayed out-of-order transactions.
    */
  protected def replayMissingRevisionsDelay: FiniteDuration = 1111.milliseconds

  private class ReplayProcessor
    extends DefaultMonotonicReplayProcessor[ID, EVT, S](store, replayProcessorCompletionTimeout, ExecutionContext.fromExecutorService(scheduler, reportFailure), replayProcessorWriteBatchSize, newPartitionedExecutionContext, newReplayMap) {
    protected def process(tx: TXN, state: Option[S]): S = ???
    override protected def processAsync(tx: TXN, state: Option[S]): Future[S] =
      DefaultEventSourceConsumer.this.processAsync(tx, state)
  }

  private class LiveProcessor(es: ES)
    extends DefaultMonotonicProcessor[ID, EVT, S](es, store, replayMissingRevisionsDelay, scheduler, newPartitionedExecutionContext) {
    protected def onUpdate(id: ID, update: Update) = onSnapshotUpdate(id, update.snapshot, update.contentUpdated)
    protected def process(tx: TXN, state: Option[S]): S = ???
    override protected def processAsync(tx: TXN, state: Option[S]): Future[S] =
      DefaultEventSourceConsumer.this.processAsync(tx, state)

  }

  protected def replayProcessor(es: ES): StreamConsumer[TXN, Future[ReplayResult]] = this match {
    case js: JoinState[ID, EVT, S] =>
      new ReplayProcessor with JoinStateProcessor[ID, EVT, S, S] {
        def preprocess(streamId: ID, streamRevision: Int, tick: Long, evt: EVT, metadata: Map[String, String]): Map[ID, Processor] = ???
        override def preprocess(txn: TXN)(implicit ev: ClassTag[EVT]): Map[ID, Processor] =
          js.preprocess(txn)(ev)
      }
    case _ =>
      new ReplayProcessor
  }

  protected def liveProcessor(es: ES, replayResult: Option[ReplayResult]): MonotonicProcessor[ID, EVT, S] =
    this match {
      case js: JoinState[ID, EVT, S] =>
        new LiveProcessor(es) with JoinStateProcessor[ID, EVT, S, S] {
          def preprocess(streamId: ID, streamRevision: Int, tick: Long, evt: EVT, metadata: Map[String, String]): Map[ID, Processor] = ???
          override def preprocess(txn: TXN)(implicit ev: ClassTag[EVT]): Map[ID, Processor] =
            js.preprocess(txn)(ev)
        }
      case _ =>
        new LiveProcessor(es)
    }

}
