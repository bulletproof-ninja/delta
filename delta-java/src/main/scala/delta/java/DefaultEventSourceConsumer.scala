package delta.java

import java.util.concurrent.ScheduledExecutorService

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import delta.util.StreamProcessStore

/**
  * [[delta.util.DefaultEventSourceConsumer]], adapted for Java.
  * @param processStore The stream process store used to track stream progress
  * @param batchProcessorWriteBatchSize Batch size when writing batch processed state to store
  * @param batchProcessorWriteCompletionTimeout Timeout after replay has completed
  * @param scheduler The scheduler used to schedule replay of potentially missing revisions, as well as general executor
  * @param evtTag The class tag for event type
  */
abstract class DefaultEventSourceConsumer[ID, EVT, S >: Null](
    processStore: StreamProcessStore[ID, S],
    scheduler: ScheduledExecutorService,
    batchProcessorWriteBatchSize: Int)(
    implicit evtTag: ClassTag[EVT])
  extends delta.util.DefaultEventSourceConsumer[ID, EVT, S](processStore, scheduler, batchProcessorWriteBatchSize) {

  def this(
      processStore: StreamProcessStore[ID, S],
      scheduler: ScheduledExecutorService,
      batchProcessorWriteBatchSize: Int,
      evtType: Class[_ <: EVT]) =
    this(processStore, scheduler, batchProcessorWriteBatchSize)(ClassTag(evtType))

  /** Turn Scala `List` into Java `Iterable`. */
  protected def iterable(list: List[_ >: EVT]): java.lang.Iterable[EVT] = {
    new java.lang.Iterable[EVT] {
      def iterator() = (list.iterator.collect { case evt: EVT => evt }).asJava
    }
  }

}
