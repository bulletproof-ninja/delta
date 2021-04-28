package delta.hazelcast

import scala.jdk.CollectionConverters._
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

import com.hazelcast.core.IMap

import scuff.concurrent.PartitionedExecutionContext

import delta.Projector
import delta.process._

/**
 * @tparam ID The key identifier
 * @tparam EVT The event type
 * @tparam S The state type
 */
abstract class HzIdempotentConsumer[ID, EVT: ClassTag, S >: Null, U](
  implicit
  ec: ExecutionContext)
extends EventSourceConsumer[ID, EVT] {

  protected type LiveResult = EntryUpdateResult

  protected def imap: IMap[ID, _ <: EntryState[S, EVT]]
  protected def persistentStore: StreamProcessStore[ID, S, U]
  def name = persistentStore.name
  protected def tickWatermark = persistentStore.tickWatermark

  protected def projector(tx: Transaction): Projector[S, EVT]
  protected def reportFailure(th: Throwable): Unit
  /** Event source streams selector. */
  protected def selector(es: EventSource): es.StreamsSelector

  /** Partitions on stream id. Defaults to `availableProcessors - 1`. */
  protected def newPartitionedExecutionContext: PartitionedExecutionContext = {
    val numThreads = 1 max (Runtime.getRuntime.availableProcessors - 1)
    PartitionedExecutionContext(numThreads, failureReporter = reportFailure _)
  }

  import delta.process.ReplayState

  /**
    * Instantiate new concurrent map used to hold state during
    * replay processing. This can be overridden to provide a
    * different implementation that e.g. stores to local disk,
    * if data set is too large for in-memory handling.
    */
  protected def newReplayMap: collection.concurrent.Map[ID, ReplayState[S]] =
    new java.util.concurrent.ConcurrentHashMap[ID, ReplayState[S]].asScala

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
  protected def replayProcessor(es: EventSource, config: ReplayProcessConfig) = {
    val projector = Projector(this.projector) _

    new HzMonotonicReplayProcessor[ID, EVT, S, U](
        persistentStore,
        config,
        newPartitionedExecutionContext,
        newReplayMap) {
      def process(tx: Transaction, currState: Option[S]) =
        projector(tx, currState)
    }

  }

  protected def liveProcessor(es: EventSource, config: LiveProcessConfig): LiveProcessor =
    new HzMonotonicProcessor[ID, EVT, S](
      es, imap, projector, reportFailure, config)

}