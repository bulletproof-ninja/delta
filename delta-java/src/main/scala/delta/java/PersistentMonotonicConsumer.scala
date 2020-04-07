package delta.java

import java.util.concurrent.ScheduledExecutorService

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import scala.concurrent.duration._
import java.util.function.Consumer
import delta.process.StreamProcessStore
import delta.process.MonotonicJoinState
import delta.process.MissingRevisionsReplay
import scala.concurrent.ExecutionContext

/**
 * [[delta.util.DefaultEventSourceConsumer]], adapted for Java.
 * @param processStore The stream process store used to track stream progress
 * @param replayProcessorWriteBatchSize Batch size when writing replay processed state to store
 * @param replayProcessorWriteCompletionTimeout Timeout after replay has completed
 * @param replayMissingScheduler The scheduler used to schedule replay of potentially missing revisions
 * @param evtTag The class tag for event type
 */
abstract class PersistentMonotonicConsumer[ID, EVT, S >: Null, U](
  protected val processStore: StreamProcessStore[ID, S, U],
  protected val replayPersistenceContext: ExecutionContext,
  protected val replayMissingScheduler: ScheduledExecutorService)(
  implicit
  evtTag: ClassTag[EVT])
extends delta.process.PersistentMonotonicConsumer[ID, EVT, S, U] {

  def this(
      processStore: StreamProcessStore[ID, S, U],
      replayPersistenceContext: ExecutionContext,
      replayMissingScheduler: ScheduledExecutorService,
      evtType: Class[_ <: EVT]) =
    this(processStore, replayPersistenceContext, replayMissingScheduler)(ClassTag(evtType))

  /** Turn Scala `List` into Java `Iterable`. */
  protected def iterable(list: List[_ >: EVT]): java.lang.Iterable[EVT] = {
    new java.lang.Iterable[EVT] {
      def iterator() = (list.iterator.collect { case evt: EVT => evt }).asJava
    }
  }

}

abstract class PersistentMonotonicJoinConsumer[ID, EVT, S >: Null, U](
    processStore: StreamProcessStore[ID, S, U],
    persistenceContext: ExecutionContext,
    scheduler: ScheduledExecutorService)(
    implicit
    evtTag: ClassTag[EVT])
  extends PersistentMonotonicConsumer[ID, EVT, S, U](processStore, persistenceContext, scheduler)
  with MonotonicJoinState[ID, EVT, S, U]
  with MissingRevisionsReplay[ID, EVT] {

  def this(
      processStore: StreamProcessStore[ID, S, U],
      persistenceContext: ExecutionContext,
      scheduler: ScheduledExecutorService,
      evtType: Class[_ <: EVT]) =
    this(processStore, persistenceContext, scheduler)(ClassTag(evtType))

  override type Snapshot = delta.Snapshot[S]
  override type Update = delta.process.Update[U]

  protected def replayMissingRevisions(
      es: delta.EventSource[ID, _ >: EVT], replayDelayLength: Long, replayDelayUnit: TimeUnit,
      scheduler: ScheduledExecutorService, reportFailure: Consumer[Throwable],
      id: ID, missing: Range,
      replayProcess: Consumer[delta.Transaction[ID, _ >: EVT]]): Unit =
    this.replayMissingRevisions(
      es, FiniteDuration(replayDelayLength, replayDelayUnit), scheduler, reportFailure.accept)(
      id, missing)(
      replayProcess.accept)

}
