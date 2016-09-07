package ulysses.util

import scala.reflect.{ ClassTag, classTag }
import java.lang.reflect.Method
import ulysses.EventContext

/**
  * Will decode events, based on methods matching
  * event name with 2 arguments, first being `Short` version,
  * second being the `SF` type, and returning type of `EVT`
  * (specific sub-type expected).
  */
abstract class ReflectiveEventDecoding[EVT: ClassTag, SF <: AnyRef: ClassTag] {
  evtCtx: EventContext[EVT, _, SF] =>

  private[this] val decoderMethods: Map[String, Method] = {
    val ShortClass = classOf[Short]
    val EvtClass = classTag[EVT].runtimeClass
    val FmtClass = classTag[SF].runtimeClass
    getClass.getMethods.filter { m =>
      val parms = m.getParameterTypes
      parms.length == 2 &&
        parms(0) == ShortClass &&
        parms(1).isAssignableFrom(FmtClass) &&
        EvtClass.isAssignableFrom(m.getReturnType)
    }.map(m => evtCtx.name(m.getReturnType.asInstanceOf[Class[EVT]]) -> m).toMap
  }

  final def decode(name: String, version: Short, data: SF): EVT =
    decoderMethods(name).invoke(this, java.lang.Short.valueOf(version), data).asInstanceOf[EVT]
}
