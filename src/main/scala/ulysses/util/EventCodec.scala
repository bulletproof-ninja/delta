package ulysses.util

import scuff.Codec
import java.lang.reflect.Method
import scala.reflect.ClassTag

trait EventCodec[EVT, FMT] {

  def eventVersion(evt: EVT): Short
  def eventName(evt: EVT): String

  def decodeEvent(name: String, version: Short, data: FMT): EVT
  def encodeEvent(evt: EVT): FMT

}

/**
 * Will decode events, based on methods matching
 * event name with 2 arguments, first being `Short` version,
 * second being the `FMT` type, and returning type of `EVT`
 * (or sub-type).
 */
trait ReflectiveEventDecoding[EVT, FMT <: AnyRef] { codec: EventCodec[EVT, FMT] =>

  protected def evtTag: ClassTag[EVT]
  protected def fmtTag: ClassTag[FMT]

  private[this] val decoderMethods: Map[String, Method] = {
    val ShortClass = classOf[Short]
    val EvtClass = evtTag.runtimeClass
    val FmtClass = fmtTag.runtimeClass
    getClass.getMethods.filter { m =>
      val parms = m.getParameterTypes
      parms.length == 2 &&
        parms(0) == ShortClass &&
        parms(1).isAssignableFrom(FmtClass) &&
        EvtClass.isAssignableFrom(m.getReturnType)
    }.map(m => eventName(m) -> m).toMap
  }

  protected def eventName(method: Method): String = method.getName

  final def decodeEvent(name: String, version: Short, data: FMT): EVT =
    decoderMethods(name).invoke(this, java.lang.Short.valueOf(version), data).asInstanceOf[EVT]
}
