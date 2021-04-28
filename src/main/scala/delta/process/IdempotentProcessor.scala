package delta.process

import scuff.concurrent._
// import scala.reflect.ClassTag
import delta.EventSource

abstract class IdempotentProcessor[SID, EVT, S >: Null, U](
  es: EventSource[SID, _ >: EVT],
  config: LiveProcessConfig,
  partitionThreads: PartitionedExecutionContext)
extends MonotonicProcessor[SID, EVT, S, U]
with MissingRevisionsReplay[SID, EVT] {

  def this(
      es: EventSource[SID, _ >: EVT],
      config: LiveProcessConfig,
      reportFailure: Throwable => Unit,
      processingThreads: Int = 1.max(Runtime.getRuntime.availableProcessors - 1),
      maxBlockingQueueSize: Int = Int.MaxValue) =
    this(es, config,
      PartitionedExecutionContext(
        processingThreads, maxBlockingQueueSize,
        Threads.newThreadGroup(
          "default-live-processor",
          daemon = false, reportFailure)))

  protected def processContext(id: SID) = partitionThreads.singleThread(id.hashCode)

  private[this] val replay = replayMissingRevisions(es, config.onMissingRevision) _

  protected def onMissingRevisions(id: SID, missing: Range): Unit =
    replay(id, missing)(this)
      .failed
      .foreach(partitionThreads.reportFailure)(processContext(id))

}
