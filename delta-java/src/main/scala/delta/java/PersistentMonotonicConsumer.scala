package delta.java

import java.util.concurrent.ScheduledExecutorService

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import scuff.concurrent.Threads
import scala.concurrent.duration._
import java.util.function.Consumer
import delta.process.StreamProcessStore
import delta.process.MonotonicJoinState
import delta.process.MissingRevisionsReplay
import delta.EventSource
import delta.Transaction

/**
 * [[delta.util.DefaultEventSourceConsumer]], adapted for Java.
 * @param processStore The stream process store used to track stream progress
 * @param replayProcessorWriteBatchSize Batch size when writing replay processed state to store
 * @param replayProcessorWriteCompletionTimeout Timeout after replay has completed
 * @param scheduler The scheduler used to schedule replay of potentially missing revisions, as well as general executor
 * @param evtTag The class tag for event type
 */
abstract class PersistentMonotonicConsumer[ID, EVT, S >: Null](
    processStore: StreamProcessStore[ID, S],
    scheduler: ScheduledExecutorService)(
    implicit
    evtTag: ClassTag[EVT])
  extends delta.process.PersistentMonotonicConsumer[ID, EVT, S](processStore, scheduler) {

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

abstract class PersistentMonotonicJoinConsumer[ID, EVT, S >: Null](
    processStore: StreamProcessStore[ID, S],
    scheduler: ScheduledExecutorService)(
    implicit
    evtTag: ClassTag[EVT])
  extends PersistentMonotonicConsumer[ID, EVT, S](processStore, scheduler)
  with MonotonicJoinState[ID, EVT, S]
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
  override type SnapshotUpdate = delta.process.SnapshotUpdate[S]

  protected def replayMissingRevisions(
      es: EventSource[ID, _ >: EVT], replayDelayLength: Long, replayDelayUnit: TimeUnit,
      scheduler: ScheduledExecutorService, reportFailure: Consumer[Throwable],
      id: ID, missing: Range,
      replayProcess: Consumer[Transaction[ID, _ >: EVT]]): Unit =
    this.replayMissingRevisions(
      es, FiniteDuration(replayDelayLength, replayDelayUnit), scheduler, reportFailure.accept)(
      id, missing)(
      replayProcess.accept)

}
