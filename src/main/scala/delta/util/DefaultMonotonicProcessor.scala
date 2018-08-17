package delta.util

import scuff.concurrent._
import scala.reflect.ClassTag
import scala.concurrent.duration.FiniteDuration
import delta.EventSource
import java.util.concurrent.ScheduledExecutorService

abstract class DefaultMonotonicProcessor[ID, EVT: ClassTag, S >: Null](
    es: EventSource[ID, _ >: EVT],
    store: StreamProcessStore[ID, S],
    replayMissingRevisionsDelay: FiniteDuration,
    scheduler: ScheduledExecutorService,
    partitionThreads: PartitionedExecutionContext)
  extends MonotonicProcessor[ID, EVT, S](store)
  with MissingRevisionsReplay[ID, EVT] {

  def this(
      es: EventSource[ID, _ >: EVT],
      store: StreamProcessStore[ID, S],
      replayMissingRevisionsDelay: FiniteDuration,
      scheduler: ScheduledExecutorService,
      reportFailure: Throwable => Unit,
      processingThreads: Int = 1.max(Runtime.getRuntime.availableProcessors - 1)) =
    this(es, store, replayMissingRevisionsDelay, scheduler,
      PartitionedExecutionContext(processingThreads, reportFailure, Threads.factory(s"default-replay-processor")))

  protected def processingContext(id: ID) = partitionThreads.singleThread(id.hashCode)

  private[this] val replay = onMissingRevisions(es, scheduler, partitionThreads.reportFailure) _
  protected def onMissingRevisions(id: ID, missing: Range): Unit =
    replay(id, missing, replayMissingRevisionsDelay)(apply)

}
