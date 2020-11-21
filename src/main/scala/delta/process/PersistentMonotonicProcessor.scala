package delta.process

import scuff.concurrent._
import scala.reflect.ClassTag
import delta.EventSource

abstract class PersistentMonotonicProcessor[ID, EVT: ClassTag, S >: Null, U](
  es: EventSource[ID, _ >: EVT],
  config: LiveProcessConfig,
  partitionThreads: PartitionedExecutionContext)
extends MonotonicProcessor[ID, EVT, S, U]
with MissingRevisionsReplay[ID, EVT] {

  def this(
      es: EventSource[ID, _ >: EVT],
      config: LiveProcessConfig,
      reportFailure: Throwable => Unit,
      processingThreads: Int = 1.max(Runtime.getRuntime.availableProcessors - 1)) =
    this(es, config,
      PartitionedExecutionContext(processingThreads, reportFailure, Threads.factory(s"default-live-processor", reportFailure)))

  protected def processContext(id: ID) = partitionThreads.singleThread(id.hashCode)

  private[this] val replay =
    replayMissingRevisions(es, Some(config.replayMissingDelay -> config.replayMissingScheduler)) _

  protected def onMissingRevisions(id: ID, missing: Range): Unit =
    replay(id, missing)(this)
      .failed
      .foreach(partitionThreads.reportFailure)(processContext(id))

}
