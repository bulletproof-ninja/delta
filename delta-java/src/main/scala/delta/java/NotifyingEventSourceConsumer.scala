package delta.java

import delta.util._
import java.util.concurrent.ScheduledExecutorService
import scala.reflect.ClassTag
import java.util.function.BiConsumer
import scala.concurrent._
import scuff.Subscription

/**
  * [[delta.util.DefaultEventSourceConsumer]] with [[delta.util.NotificationSupport]],
  * adapted for Java.
  * @param processStore The stream process store used to track stream progress
  * @param replayProcessorWriteBatchSize Batch size when writing replay processed state to store
  * @param replayProcessorWriteCompletionTimeout Timeout after replay has completed
  * @param scheduler The scheduler used to schedule replay of potentially missing revisions, as well as general executor
  * @param evtTag The class tag for event type
  */
abstract class NotifyingEventSourceConsumer[ID, EVT, S >: Null, FMT](
    processStore: StreamProcessStore[ID, S],
    scheduler: ScheduledExecutorService,
    replayProcessorWriteBatchSize: Int)(
    implicit evtTag: ClassTag[EVT])
  extends DefaultEventSourceConsumer[ID, EVT, S](processStore, scheduler, replayProcessorWriteBatchSize)
  with NotificationSupport[ID, S, FMT] {

  type FormatKey = Object

  def this(
      processStore: StreamProcessStore[ID, S],
      scheduler: ScheduledExecutorService,
      replayProcessorWriteBatchSize: Int,
      evtType: Class[_ <: EVT]) =
    this(processStore, scheduler, replayProcessorWriteBatchSize)(ClassTag(evtType))

  protected def subscribe[T <: S](key: Object, id: ID, notificationCtx: ExecutionContext, limitTo: Class[_ <: T], consumer: BiConsumer[delta.Snapshot[T], FMT]): Future[Subscription] = {
    subscribe(notificationCtx)(id :: Nil, key) {
      case (_, snapshot, formatted) if limitTo.isInstance(snapshot.content) =>
        consumer.accept(snapshot.asInstanceOf[delta.Snapshot[T]], formatted)
    }
  }
  protected def subscribe(key: Object, id: ID, notificationCtx: ExecutionContext, consumer: BiConsumer[Snapshot, FMT]): Future[Subscription] = {
    subscribe(notificationCtx)(id :: Nil, key) {
      case (_, snapshot, formatted) => consumer.accept(snapshot, formatted)
    }
  }

}
