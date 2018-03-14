package delta.java

import java.util.Optional

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
  * [[delta.util.EventSourceProcessor]], adapted for Java.
  */
abstract class EventSourceConsumer[ID, EVT, CH](maxTickProcessed: Option[Long])(implicit evtTag: ClassTag[EVT])
  extends delta.util.EventSourceConsumer[ID, EVT, CH](maxTickProcessed) {

  def this(maxTickProcessed: Optional[java.lang.Long], evtType: Class[_ <: EVT]) =
    this(maxTickProcessed: Option[Long])(ClassTag(evtType))

  def this(maxTickProcessed: java.lang.Long, evtType: Class[_ <: EVT]) =
    this(Option(maxTickProcessed).map(_.longValue))(ClassTag(evtType))

  type BatchResult = Object

  protected def batchProcessor(es: ES): BatchProcessor[ID, EVT]
  protected def realtimeProcessor(es: ES, batchResult: Option[Object]): RealtimeProcessor[ID, EVT]

  /** Turn Scala `List` into Java `Iterable`. */
  protected def asJava(list: List[_ >: EVT]): java.lang.Iterable[EVT] = {
    new java.lang.Iterable[EVT] {
      def iterator() = (list.iterator.collect { case evt: EVT => evt }).asJava
    }
  }

}
