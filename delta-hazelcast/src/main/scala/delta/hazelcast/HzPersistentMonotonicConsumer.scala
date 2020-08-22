package delta.hazelcast

import java.util.concurrent.ScheduledExecutorService

import scala.jdk.CollectionConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.ClassTag

import com.hazelcast.core.IMap

import delta.Projector
import delta.process._
import scuff.concurrent.PartitionedExecutionContext

/**
 * @tparam ID The key identifier
 * @tparam EVT The event type
 * @tparam S The state type
 */
abstract class HzPersistentMonotonicConsumer[ID, EVT: ClassTag, S >: Null: ClassTag](
  implicit
  ec: ExecutionContext,
  scheduler: ScheduledExecutorService)
extends EventSourceConsumer[ID, EVT] {

  protected type LiveResult = EntryUpdateResult

  protected def imap: IMap[ID, _ <: EntryState[S, EVT]]
  protected def tickWatermark: Option[Tick]

  /**
   * If revisions are detected missing during live
   * processing, this is the delay before requesting
   * replay. If the messaging infrastructure is not
   * guaranteed to be ordered per stream, a delay can
   * prevent unnecessary replay.
   */
  protected def missingRevisionsReplayDelay: FiniteDuration

  /**
   * How long to wait for replay processing to finish,
   * once all transactions have been replayed.
   */
  protected def finalizeReplayProcessingTimeout: FiniteDuration

  protected def projector(tx: Transaction): Projector[S, EVT]
  protected def reportFailure(th: Throwable): Unit
  /** Event source streams selector. */
  protected def selector(es: EventSource): es.StreamsSelector

  /** Partitions on stream id. Defaults to `availableProcessors - 1`. */
  protected def newPartitionedExecutionContext: PartitionedExecutionContext = {
    val numThreads = 1 max (Runtime.getRuntime.availableProcessors - 1)
    PartitionedExecutionContext(numThreads, failureReporter = reportFailure)
  }

  import delta.process.ConcurrentMapStore.State

  /**
    * Instantiate new concurrent map used to hold state during
    * replay processing. This can be overridden to provide a
    * different implementation that e.g. stores to local disk,
    * if data set is too large for in-memory handling.
    */
  protected def newReplayMap: collection.concurrent.Map[ID, State[S]] =
    new java.util.concurrent.ConcurrentHashMap[ID, State[S]].asScala

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
    * @note Tick order is arbitrary, thus no guarantee
    * of causal tick processing, between different streams,
    * although, as mentioned, individual streams will be
    * processed in causal (monotonic revision) order.
    * If the event source is empty, this method will
    * not be called.
    *
    * It is highly recommended to return an instance of
    * [[delta.process.MonotonicReplayProcessor]] here.
    */
  protected def replayProcessor(es: EventSource) = {
    val projector = Projector(this.projector) _

    new HzMonotonicReplayProcessor[ID, EVT, S, Unit](
        tickWatermark,
        imap,
        finalizeReplayProcessingTimeout,
        newPartitionedExecutionContext,
        newReplayMap) {
      def process(tx: Transaction, currState: Option[S]) = projector(tx, currState)
      def tickWindow = Some(HzPersistentMonotonicConsumer.this.tickWindow)
    }

  }

  protected def liveProcessor(es: EventSource): LiveProcessor =
    new HzMonotonicProcessor[ID, EVT, S](
      es, imap, projector, reportFailure,
      missingRevisionsReplayDelay)

}
