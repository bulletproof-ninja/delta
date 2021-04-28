package delta.process

import scala.concurrent._
import scuff.concurrent._
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
 * high-water mark (subject to the tick window).
 * @tparam SID The stream id type
 * @tparam EVT The process specific event type
 * @tparam InUse The processing (work) state representation
 * @tparam U The update type. Is often the same as `InUse`, unless a smaller diff type is desired.
 */
abstract class IdempotentProcessing[SID, EVT, InUse >: Null, U]
extends EventSourceProcessing[SID, EVT]
with TransactionProcessor[SID, EVT, InUse] {

  protected def adHocExeCtx: ExecutionContext

  override protected type Transaction = delta.Transaction[SID, _ >: EVT]
  protected type Update = delta.process.Update[U]

  protected def processStore: StreamProcessStore[SID, InUse, U]
  def name = s"${processStore.name}-processor"

  /**
    * Callback on snapshot updates from live processing.
    * @param id The stream id
    * @param update The snapshot update
    */
  protected def onUpdate(id: SID, update: Update): Unit

  protected def tickWatermark: Option[Tick] = processStore.tickWatermark

  private lazy val defaultThreadGroup =
    Threads.newThreadGroup(
      name = name, daemon = false,
      reportFailure = this.adHocExeCtx.reportFailure)

  protected def threadGroup = defaultThreadGroup

  /** Partitions on stream id. Defaults to `availableProcessors - 1`. */
  protected def newPartitionedExecutionContext(
      replay: Boolean,
      maxBlockingQueueSize: Int)
      : PartitionedExecutionContext = {
    val numThreads = 1 max (Runtime.getRuntime.availableProcessors - 1)
    val suffix = if (replay) "replay" else "live"
    val tg = threadGroup
    val tf = Threads.factory(s"${tg.getName}-$suffix", tg)
    PartitionedExecutionContext(numThreads, maxBlockingQueueSize, tg, tf, _.hashCode)
  }

  /**
    * Instantiate new concurrent map used to hold state during
    * replay processing. This can be overridden to provide a
    * different implementation that e.g. stores to local disk,
    * if data set is too large for in-memory handling.
    */
  protected def newReplayMap: CMap[SID, ReplayState[InUse]] =
    new TrieMap[SID, ReplayState[InUse]]

  protected type LiveResult = InUse

  protected class ReplayProc(
    config: ReplayProcessConfig,
    protected val completionContext: ExecutionContext)
  extends IdempotentReplayProcessor[SID, EVT, InUse, U](
      processStore,
      config,
      newPartitionedExecutionContext(replay = true, config.maxBlockingQueueSize), newReplayMap) {

    protected def process(tx: Transaction, state: Option[InUse]): Future[InUse] =
      IdempotentProcessing.this.process(tx, state)

  }

  protected class LiveProc(
    es: EventSource,
    config: LiveProcessConfig)
  extends IdempotentProcessor[SID, EVT, InUse, U](
      es, config,
      newPartitionedExecutionContext(replay = false, Int.MaxValue)) {
    def name = s"${processStore.name}-live-processor"
    protected def processStore = IdempotentProcessing.this.processStore
    protected def onUpdate(id: SID, update: Update) =
      IdempotentProcessing.this.onUpdate(id, update)
    protected def process(tx: Transaction, state: Option[InUse]): Future[InUse] =
      IdempotentProcessing.this.process(tx, state)

  }

  protected def replayProcessor(es: EventSource, config: ReplayProcessConfig): ReplayProcessor =
    new ReplayProc(config, adHocExeCtx)

  protected def liveProcessor(es: EventSource, config: LiveProcessConfig): LiveProcessor =
    new LiveProc(es, config)

}
