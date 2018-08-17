package delta.hazelcast

import delta.util.EventSourceConsumer
import com.hazelcast.core.IMap
import scuff.Codec
import scala.concurrent._
import scala.concurrent.duration._
import scuff.concurrent.PartitionedExecutionContext
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import delta.EventReducer
import java.util.concurrent.ScheduledExecutorService
import delta.Snapshot

/**
 * @tparam ID The key identifier
 * @tparam EVT The event type
 * @tparam WS The working state type
 * @tparam RS The resting state type
 */
abstract class HzEventSourceConsumer[ID, EVT: ClassTag, WS >: Null, RS >: Null](
    protected val imap: IMap[ID, EntryState[RS, EVT]],
    stateCodec: Codec[RS, WS],
    reducer: EventReducer[WS, EVT],
    protected val tickWatermark: Option[Long],
    replayProcessingCompletionTimeout: FiniteDuration,
    scheduler: ScheduledExecutorService)
  extends EventSourceConsumer[ID, EVT] {

  protected type ReplayResult = Any

  protected def reportFailure(th: Throwable): Unit
  /** Event source selector. */
  protected def selector(es: ES): es.CompleteSelector

  protected def executionContext: ExecutionContext = ExecutionContext.fromExecutorService(scheduler, reportFailure)

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
  protected def newReplayMap: collection.concurrent.Map[ID, Snapshot[WS]] =
    new java.util.concurrent.ConcurrentHashMap[ID, Snapshot[WS]].asScala

  /**
    * Called at startup, when replay processing of
    * transactions begins. Unlike live processing,
    * there will be no duplicate revisions passed
    * to replay processor.
    *
    * Replay processing enables in-memory processing,
    * where persistence can be delayed until done,
    * making large data sets much faster to process.
    *
    * NOTE: Tick order is arbitrary, thus no guarantee
    * of causal tick processing, between different streams,
    * although, as mentioned, individual streams will be
    * processed in causal (monotonic revision) order.
    * If the event source is empty, this method will
    * not be called.
    *
    * It is highly recommended to return an instance of
    * [[delta.util.MonotonicReplayProcessor]] here.
    */

  private[this] val reduce = EventReducer.process(reducer) _
  protected def replayProcessor(es: ES) =
    new HzMonotonicReplayProcessor[ID, EVT, WS, RS](
      imap,
      stateCodec,
      replayProcessingCompletionTimeout,
      executionContext,
      newPartitionedExecutionContext,
      newReplayMap) {
    def process(txn: TXN, currState: Option[WS]): WS = reduce(currState, txn.events)
  }

  protected def missingRevisionsReplayDelay: FiniteDuration = 1111.milliseconds

  protected def liveProcessor(es: ES, replayResult: Option[ReplayResult]): TXN => _ = {
    new HzMonotonicProcessor[ID, EVT, WS, RS](
      es, imap, stateCodec, reducer, reportFailure,
      scheduler, missingRevisionsReplayDelay)
  }

}
