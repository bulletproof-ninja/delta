package delta.hazelcast

import java.util.concurrent.ScheduledExecutorService

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.ClassTag

import com.hazelcast.core.IMap

import delta.TransactionProjector
import delta.process.EventSourceConsumer
import scuff.concurrent.PartitionedExecutionContext

/**
 * @tparam ID The key identifier
 * @tparam EVT The event type
 * @tparam WS The working state type
 * @tparam RS The resting state type
 */
abstract class HzPersistentMonotonicConsumer[ID, EVT: ClassTag, S >: Null: ClassTag](
    protected val imap: IMap[ID, EntryState[S, EVT]],
    txnProjector: TransactionProjector[S, EVT],
    protected val tickWatermark: Option[Long],
    finishReplayProcessingTimeout: FiniteDuration,
    executionContext: ExecutionContext,
    scheduler: ScheduledExecutorService)
  extends EventSourceConsumer[ID, EVT] {

  protected type ReplayResult = Any

  protected def reportFailure(th: Throwable): Unit
  /** Event source streams selector. */
  protected def selector(es: EventSource): es.StreamsSelector

  /** Partitions on stream id. Defaults to `availableProcessors - 1`. */
  protected def newPartitionedExecutionContext: PartitionedExecutionContext = {
    val numThreads = 1 max (Runtime.getRuntime.availableProcessors - 1)
    PartitionedExecutionContext(numThreads, failureReporter = reportFailure)
  }

  import delta.process.ConcurrentMapStore.Value
  
  /**
    * Instantiate new concurrent map used to hold state during
    * replay processing. This can be overridden to provide a
    * different implementation that e.g. stores to local disk,
    * if data set is too large for in-memory handling.
    */
  protected def newReplayMap: collection.concurrent.Map[ID, Value[S]] =
    new java.util.concurrent.ConcurrentHashMap[ID, Value[S]].asScala

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

  protected def replayProcessor(es: EventSource) =
    new HzMonotonicReplayProcessor[ID, EVT, S](
      tickWatermark,
      imap,
      finishReplayProcessingTimeout,
      executionContext,
      newPartitionedExecutionContext,
      newReplayMap) {
    def process(txn: TXN, currState: Option[S]) = txnProjector(txn, currState)
  }

  protected def missingRevisionsReplayDelay: FiniteDuration = 2222.millis

  protected def liveProcessor(es: EventSource, replayResult: Option[ReplayResult]): TXN => Any = {
    new HzMonotonicProcessor[ID, EVT, S](
      es, imap, txnProjector, reportFailure,
      scheduler, missingRevisionsReplayDelay)
  }

}
