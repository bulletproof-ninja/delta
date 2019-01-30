package delta.java

import java.util.concurrent.ScheduledExecutorService

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import delta.util.StreamProcessStore
import scuff.concurrent.Threads
import delta.util.MonotonicJoinState
import delta.util.MissingRevisionsReplay
import java.util.function.Consumer
import delta.EventSource
import scala.concurrent.duration.FiniteDuration
import delta.Transaction
import java.util.concurrent.TimeUnit

/**
 * [[delta.util.DefaultEventSourceConsumer]], adapted for Java.
 * @param processStore The stream process store used to track stream progress
 * @param replayProcessorWriteBatchSize Batch size when writing replay processed state to store
 * @param replayProcessorWriteCompletionTimeout Timeout after replay has completed
 * @param scheduler The scheduler used to schedule replay of potentially missing revisions, as well as general executor
 * @param evtTag The class tag for event type
 */
abstract class MonotonicEventSourceConsumer[ID, EVT, S >: Null](
    protected val processStore: StreamProcessStore[ID, S],
    scheduler: ScheduledExecutorService)(
    implicit
    evtTag: ClassTag[EVT])
  extends delta.util.MonotonicEventSourceConsumer[ID, EVT, S](processStore, scheduler) {

  def this(
      processStore: StreamProcessStore[ID, S],
      scheduler: ScheduledExecutorService,
      evtType: Class[_ <: EVT]) =
    this(processStore, scheduler)(ClassTag(evtType))

  def this(
      processStore: StreamProcessStore[ID, S],
      evtType: Class[_ <: EVT]) =
    this(processStore, Threads.DefaultScheduler)(ClassTag(evtType))

  /** Turn Scala `List` into Java `Iterable`. */
  protected def iterable(list: List[_ >: EVT]): java.lang.Iterable[EVT] = {
    new java.lang.Iterable[EVT] {
      def iterator() = (list.iterator.collect { case evt: EVT => evt }).asJava
    }
  }

}

abstract class MonotonicJoinStateEventSourceConsumer[ID, EVT, S >: Null, JS >: Null <: S](
    processStore: StreamProcessStore[ID, S],
    scheduler: ScheduledExecutorService)(
    implicit
    evtTag: ClassTag[EVT])
  extends MonotonicEventSourceConsumer[ID, EVT, S](processStore, scheduler)
  with MonotonicJoinState[ID, EVT, S, JS]
  with MissingRevisionsReplay[ID, EVT] {

  def this(
      processStore: StreamProcessStore[ID, S],
      scheduler: ScheduledExecutorService,
      evtType: Class[_ <: EVT]) =
    this(processStore, scheduler)(ClassTag(evtType))

  def this(
      processStore: StreamProcessStore[ID, S],
      evtType: Class[_ <: EVT]) =
    this(processStore, Threads.DefaultScheduler)(ClassTag(evtType))

  override type Snapshot = delta.Snapshot[S]
  override type SnapshotUpdate = delta.util.SnapshotUpdate[S]

  protected def onMissingRevisions(
      es: EventSource[ID, _ >: EVT], replayDelayLength: Long, replayDelayUnit: TimeUnit,
      scheduler: ScheduledExecutorService, reportFailure: Consumer[Throwable],
      id: ID, missing: Range,
      replayProcess: Consumer[Transaction[ID, _ >: EVT]]): Unit =
    this.onMissingRevisions(
      es, FiniteDuration(replayDelayLength, replayDelayUnit), scheduler, reportFailure.accept)(
      id, missing)(
      replayProcess.accept)

}
