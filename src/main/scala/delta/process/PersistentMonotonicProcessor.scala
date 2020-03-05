package delta.process

import scuff.concurrent._
import scala.reflect.ClassTag
import scala.concurrent.duration.FiniteDuration
import delta.EventSource
import java.util.concurrent.ScheduledExecutorService

abstract class PersistentMonotonicProcessor[ID, EVT: ClassTag, S >: Null, U](
    es: EventSource[ID, _ >: EVT],
    replayMissingDelay: FiniteDuration,
    replayMissingScheduler: ScheduledExecutorService,
    partitionThreads: PartitionedExecutionContext)
  extends MonotonicProcessor[ID, EVT, S, U]
  with MissingRevisionsReplay[ID, EVT] {

  def this(
      es: EventSource[ID, _ >: EVT],
      replayMissingDelay: FiniteDuration,
      replayMissingScheduler: ScheduledExecutorService,
      reportFailure: Throwable => Unit,
      processingThreads: Int = 1.max(Runtime.getRuntime.availableProcessors - 1)) =
    this(es, replayMissingDelay, replayMissingScheduler,
      PartitionedExecutionContext(processingThreads, reportFailure, Threads.factory(s"default-live-processor", reportFailure)))

  protected def processContext(id: ID) = partitionThreads.singleThread(id.hashCode)

  private[this] val replay =
    replayMissingRevisions(es, replayMissingDelay, replayMissingScheduler, partitionThreads.reportFailure) _

  protected def onMissingRevisions(id: ID, missing: Range): Unit = replay(id, missing)(this)

}
