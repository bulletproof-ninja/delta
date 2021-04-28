package delta.java

import java.util.concurrent.ScheduledExecutorService
import java.util.function._

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import delta.process._, LiveProcessConfig._


/**
 * [[delta.util.DefaultEventSourceConsumer]], adapted for Java.
 * @param processStore The stream process store used to track stream progress and store state
 * @param replayMissingScheduler Used to schedule replay of missing revisions
 * @param ec General execution context
 * @param evtTag The class tag for event type
 * @tparam ID The stream identifier
 * @tparam EVT The event type
 * @tparam S The state type
 */
abstract class IdempotentConsumer[ID, EVT, S >: Null, U](
  process: BiFunction[delta.Transaction[ID, _ >: EVT], Option[S], S],
  protected val processStore: StreamProcessStore[ID, S, U],
  protected val scheduler: ScheduledExecutorService)(
  implicit
  ec: ExecutionContext,
  evtTag: ClassTag[EVT])
extends delta.process.IdempotentProcessing[ID, EVT, S, U]
with delta.process.EventSourceConsumer[ID, EVT] {

  def this(
      process: BiFunction[delta.Transaction[ID, _ >: EVT], Option[S], S],
      processStore: StreamProcessStore[ID, S, U],
      scheduler: ScheduledExecutorService,
      ec: ExecutionContext,
      evtType: Class[_ <: EVT]) =
    this(process, processStore, scheduler)(ec, ClassTag(evtType))

  protected def process(tx: Transaction, currState: Option[S]): Future[S] =
    this.process.apply(tx, currState)

  /** Turn Scala `List` into Java `Iterable`. */
  protected def iterable(list: List[_ >: EVT]): java.lang.Iterable[EVT] = {
    new java.lang.Iterable[EVT] {
      def iterator() = (list.iterator.collect { case evt: EVT => evt }).asJava
    }
  }

}

abstract class IdempotentJoinConsumer[ID, EVT, S >: Null, U](
  process: BiFunction[delta.Transaction[ID, _ >: EVT], Option[S], S],
  processStore: StreamProcessStore[ID, S, U],
  scheduler: ScheduledExecutorService)(
  implicit
  ec: ExecutionContext,
  evtTag: ClassTag[EVT])
extends IdempotentConsumer[ID, EVT, S, U](process, processStore, scheduler)
with MonotonicJoinProcessor[ID, EVT, S, U]
with MissingRevisionsReplay[ID, EVT] {

  def this(
      process: BiFunction[delta.Transaction[ID, _ >: EVT], Option[S], S],
      processStore: StreamProcessStore[ID, S, U],
      scheduler: ScheduledExecutorService,
      ec: ExecutionContext,
      evtType: Class[_ <: EVT]) =
    this(process, processStore, scheduler)(ec, ClassTag(evtType))

  override type Snapshot = delta.Snapshot[S]
  override type Update = delta.process.Update[U]
  override def name = super.name

  protected def replayMissingRevisions(
      es: delta.EventSource[ID, _ >: EVT], replayDelayLength: Long, replayDelayUnit: TimeUnit,
      scheduler: ScheduledExecutorService, reportFailure: Consumer[Throwable],
      id: ID, missing: Range,
      replayProcess: Consumer[delta.Transaction[ID, _ >: EVT]]): Unit =
    this.replayMissingRevisions(
      es,
      DelayedReplay(FiniteDuration(replayDelayLength, replayDelayUnit), scheduler))(
      id, missing)(
      replayProcess.accept)
    .failed
    .foreach(reportFailure.accept)

}
