package delta.java

import delta.Tick

import java.util.Optional

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

/**
  * [[delta.process.PersistentConsumer]], adapted for Java.
  */
abstract class EventSourceConsumer[ID, EVT](
  val tickWatermark: Option[Tick])(implicit evtTag: ClassTag[EVT])
extends delta.process.EventSourceProcessing[ID, EVT]
with delta.process.EventSourceConsumer[ID, EVT] {

  def this(tickWatermark: Optional[java.lang.Long], evtType: Class[_ <: EVT]) =
    this(tickWatermark: Option[Tick])(ClassTag(evtType))

  def this(tickWatermark: java.lang.Long, evtType: Class[_ <: EVT]) =
    this(Option(tickWatermark).map(_.longValue))(ClassTag(evtType))

  protected def replayProcessor(es: EventSource): this.ReplayProcessor
  protected def liveProcessor(es: EventSource): this.LiveProcessor

  /** Turn Scala `List` of events into Java `Iterable`. */
  protected def iterable(list: List[_ >: EVT]): java.lang.Iterable[EVT] = {
    new java.lang.Iterable[EVT] {
      def iterator() = (list.iterator.collect { case evt: EVT => evt }).asJava
    }
  }

}
