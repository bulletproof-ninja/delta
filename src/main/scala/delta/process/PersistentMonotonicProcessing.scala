package delta.process

import scala.concurrent._
import scuff.concurrent._
import scala.reflect.ClassTag
import scala.concurrent.duration._
import java.util.concurrent.ScheduledExecutorService
import scala.collection.concurrent.{ Map => CMap, TrieMap }

/**
 * Default monotonic implementation of [[delta.process.EventSourceProcessing]].
 * Can be started and stopped on demand, without loss of
 * transactions.
 * @note Since replay processing is generally
 * not done in tick order, a tick received
 * through incomplete replay processing
 * is NOT reliable as a high-water mark. In other words,
 * replay processing should not use a persistent
 * [[delta.process.StreamProcessStore]] until replay
 * processing has completed.
 * Any tick received in live processing can be considered a
 * high-water mark (subject to tick skew). (It's also much slower)
 * @tparam SID The stream id type
 * @tparam EVT The process specific event type
 * @tparam Work The processing state type
 * @tparam U The update type (often the same as `Work`)
 */
abstract class PersistentMonotonicProcessing[SID, EVT: ClassTag, Work >: Null, U](
  implicit ec: ExecutionContext)
extends EventSourceProcessing[SID, EVT]
with TransactionProcessor[SID, EVT, Work] {

  override protected type Transaction = delta.Transaction[SID, _ >: EVT]
  protected type Snapshot = delta.Snapshot[Work]
  protected type Update = delta.process.Update[U]

  /** Batch size when persisting result of replay. */
  protected def replayPersistenceBatchSize: Int

  protected def processStore: StreamProcessStore[SID, Work, U]

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
    * Time allowed for replay processor to finish processing
    * remaining transactions once replay has completed.
    *
    * @note If replay is not throttled and transaction processing
    * is relatively time consuming, it's likely that replay will
    * finish, but with most transactions queued in thread pools.
    * Such a condition will require a post-replay timeout that is
    * large enough to accomodate completing all transactions.
    * As such, ''it might be sensible to have this value be
    * configurable''.
    */
  protected def postReplayTimeout: FiniteDuration

  /**
    * Callback on snapshot updates from live processing.
    * @param id The stream id
    * @param update The snapshot update
    */
  protected def onUpdate(id: SID, update: Update): Unit

  protected def tickWatermark: Option[Tick] = processStore.tickWatermark

  private lazy val defaultThreadGroup =
    Threads.newThreadGroup(
      name = s"${getClass.getSimpleName}-processing",
      daemon = false, reportFailure = this.reportFailure)

  protected def threadGroup = defaultThreadGroup

  /** Partitions on stream id. Defaults to `availableProcessors - 1`. */
  protected def newPartitionedExecutionContext(replay: Boolean): PartitionedExecutionContext = {
    val numThreads = 1 max (Runtime.getRuntime.availableProcessors - 1)
    val suffix = if (replay) "replay" else "live"
    val tg = threadGroup
    val tf = Threads.factory(s"${tg.getName}-$suffix", tg)
    PartitionedExecutionContext(numThreads, tg, tf, _.hashCode)
  }

  /**
    * Instantiate new concurrent map used to hold state during
    * replay processing. This can be overridden to provide a
    * different implementation that e.g. stores to local disk,
    * if data set is too large for in-memory handling.
    */
  protected def newReplayMap: CMap[SID, ConcurrentMapStore.State[Work]] =
    new TrieMap[SID, ConcurrentMapStore.State[Work]]

  protected type LiveResult = Work

  protected class ReplayConsumer
  extends PersistentMonotonicReplayProcessor[SID, EVT, Work, U](
      processStore,
      postReplayTimeout,
      replayPersistenceBatchSize,
      newPartitionedExecutionContext(replay = true), newReplayMap) {

    override protected def process(tx: Transaction, state: Option[Work]): Future[Work] =
      PersistentMonotonicProcessing.this.process(tx, state)

  }

  protected class LiveConsumer(es: EventSource)
  extends PersistentMonotonicProcessor[SID, EVT, Work, U](
      es,
      replayMissingDelay, replayMissingScheduler,
      newPartitionedExecutionContext(replay = false)) {

    protected def processStore = PersistentMonotonicProcessing.this.processStore
    protected def onUpdate(id: SID, update: Update) =
      PersistentMonotonicProcessing.this.onUpdate(id, update)
    override protected def process(tx: Transaction, state: Option[Work]): Future[Work] =
      PersistentMonotonicProcessing.this.process(tx, state)

  }

  protected def replayProcessor(es: EventSource): ReplayProcessor =
    new ReplayConsumer

  protected def liveProcessor(es: EventSource): LiveProcessor =
    new LiveConsumer(es)

}

abstract class PersistentMonotonicConsumer[SID, EVT: ClassTag, Work >: Null, U](
  implicit ec: ExecutionContext)
extends PersistentMonotonicProcessing[SID, EVT, Work, U]
with EventSourceConsumer[SID, EVT]
