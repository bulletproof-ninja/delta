package delta.java

import java.util.Optional

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
  * [[delta.util.EventSourceProcessor]], adapted for Java.
  */
abstract class EventSourceConsumer[ID, EVT](protected val tickWatermark: Option[Long])(implicit evtTag: ClassTag[EVT])
  extends delta.util.EventSourceConsumer[ID, EVT] {

  def this(tickWatermark: Optional[java.lang.Long], evtType: Class[_ <: EVT]) =
    this(tickWatermark: Option[Long])(ClassTag(evtType))

  def this(tickWatermark: java.lang.Long, evtType: Class[_ <: EVT]) =
    this(Option(tickWatermark).map(_.longValue))(ClassTag(evtType))

  type ReplayResult = Object

  protected def replayProcessor(es: ES): ReplayProcessor[ID, EVT]
  protected def liveProcessor(es: ES, replayResult: Option[Object]): LiveProcessor[ID, EVT]

  /** Turn Scala `List` of events into Java `Iterable`. */
  protected def iterable(list: List[_ >: EVT]): java.lang.Iterable[EVT] = {
    new java.lang.Iterable[EVT] {
      def iterator() = (list.iterator.collect { case evt: EVT => evt }).asJava
    }
  }

}
