package delta.process

import scala.concurrent._
import scuff.StreamConsumer
import scuff.concurrent._
import scala.reflect.ClassTag
import scala.concurrent.duration._
import java.util.concurrent.ScheduledExecutorService
import scala.collection.concurrent.{ Map => CMap, TrieMap }

/**
 * Default monotonic implementation of [[delta.process.EventSourceConsumer]].
 * Can be started and stopped on demand, without loss of
 * transactions.
 * NOTE: Since replay processing is generally
 * not done in tick order, a tick received
 * through incomplete replay processing
 * is NOT reliable as a high-water mark. In other words,
 * replay processing should not use a persistent
 * [[delta.process.StreamProcessStore]] until replay
 * processing has completed.
 * Any tick received in live processing can be considered a
 * high-water mark (subject to tick skew).
 */
abstract class PersistentMonotonicConsumer[ID, EVT: ClassTag, S >: Null, U]
  extends EventSourceConsumer[ID, EVT]
  with TransactionProcessor[ID, EVT, S] {

  override protected type Transaction = delta.Transaction[ID, _ >: EVT]
  protected type Snapshot = delta.Snapshot[S]
  protected type Update = delta.process.Update[U]

  /** Batch size when persisting result of replay. */
  protected def replayPersistenceBatchSize: Int
  protected def replayPersistenceContext: ExecutionContext

  protected def processStore: StreamProcessStore[ID, S, U]

  protected def reportFailure(th: Throwable): Unit
  /**
    * Time delay before replaying missing revisions.
    * This allows some margin for delayed out-of-order transactions,
    * either due to choice of messaging infrastructure, or consistency
    * validation, where transaction propagation is reversed during a
    * consistency violation.
    */
    protected def replayMissingDelay: FiniteDuration
    protected def replayMissingScheduler: ScheduledExecutorService

  /**
    * Callback on snapshot updates from live processing.
    * @param id The stream id
    * @param update The snapshot update
    */
  protected def onUpdate(id: ID, update: Update): Unit

  protected def tickWatermark: Option[Long] = processStore.tickWatermark

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
  protected def newReplayMap: CMap[ID, ConcurrentMapStore.State[S]] =
    new TrieMap[ID, ConcurrentMapStore.State[S]]

  protected type ReplayResult = Any

  /**
    * Time allowed for replay processor to finish processing
    * remaining transactions once replay has completed.
    */
  protected def replayProcessorCompletionTimeout: FiniteDuration = 11.seconds

  protected class ReplayProcessor
    extends PersistentMonotonicReplayProcessor[ID, EVT, S, U](
        processStore,
        replayProcessorCompletionTimeout,
        replayPersistenceContext,
        replayPersistenceBatchSize,
        newPartitionedExecutionContext, newReplayMap) {
    override protected def process(tx: Transaction, state: Option[S]): Future[S] =
      PersistentMonotonicConsumer.this.process(tx, state)
  }

  protected class LiveProcessor(es: EventSource)
    extends PersistentMonotonicProcessor[ID, EVT, S, U](
        es,
        replayMissingDelay, replayMissingScheduler,
        newPartitionedExecutionContext) {
    protected def processStore = PersistentMonotonicConsumer.this.processStore
    protected def onUpdate(id: ID, update: Update) =
      PersistentMonotonicConsumer.this.onUpdate(id, update)
    override protected def process(tx: Transaction, state: Option[S]): Future[S] =
      PersistentMonotonicConsumer.this.process(tx, state)

  }

  protected def replayProcessor(es: EventSource): StreamConsumer[Transaction, Future[ReplayResult]] =
    new ReplayProcessor

  protected def liveProcessor(es: EventSource, replayResult: Option[ReplayResult]): Transaction => Any =
    new LiveProcessor(es)

}
