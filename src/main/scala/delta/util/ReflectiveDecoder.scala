package delta.util

import scala.reflect.{ ClassTag, classTag }
import java.lang.reflect.Method
import delta.EventCodec
import scala.compat.Platform
import delta.NoVersioning

/**
  * Will look for event decode methods, which must match
  * the following signature:
  * For versioned events (default), the method must have two
  * arguments, 1. `Byte`, 2. `SF` (the encoding format) and
  * must return an EVT type.
  * For `NoVersioning` events, the method must take a single
  * argument, `SF`, and must return a type of `EVT`.
  */
abstract class ReflectiveDecoder[EVT: ClassTag, SF <: AnyRef: ClassTag] {
  evtCtx: EventCodec[EVT, SF] =>

  private[this] lazy val decoderMethods: Map[String, Method] = {
    val noVersion = evtCtx.isInstanceOf[NoVersioning[_, _]]
    val parmCount = if (noVersion) 1 else 2
    val ByteClass = classOf[Byte]
    val EvtClass = classTag[EVT].runtimeClass
    val FmtClass = classTag[SF].runtimeClass
    val methods = getClass.getMethods.filter { m =>
      val parms = m.getParameterTypes
      parms.length == parmCount &&
        parms(parmCount - 1).isAssignableFrom(FmtClass) &&
        (noVersion || parms(0) == ByteClass) &&
        EvtClass.isAssignableFrom(m.getReturnType)
    }.map(m => evtCtx.name(m.getReturnType.asInstanceOf[Class[EVT]]) -> m)
    methods.groupBy(_._1).toSeq.filter(_._2.size > 1).headOption.foreach {
      case (evtName, methods) =>
        val methodsString = methods.mkString(Platform.EOL, Platform.EOL, "")
        throw new IllegalStateException(
          s"""Event "$evtName" has ambiguous decoding by the following methods:$methodsString""")
    }
    if (methods.isEmpty) {
      throw new IllegalStateException(s"No decoding methods found for $getClass")
    }
    methods.toMap
  }

  private def decoderNotFound(evtName: String, noVersion: Boolean): Nothing = {
    val versionArg = if (noVersion) "" else "version: Byte, "
    val signature = s"def methodName(${versionArg}data: ${classTag[SF].runtimeClass.getName}): EVT"
    val message = s"""No decoding method found for event "$evtName". Must match the following signature, where EVT is a sub-type of ${classTag[EVT].runtimeClass.getName}: $signature"""
    throw new IllegalStateException(message)
  }

  def decode(name: String, version: Byte, data: SF): EVT = {
    val decoderMethod = decoderMethods.getOrElse(name, decoderNotFound(name, noVersion = false))
    decoderMethod.invoke(this, Byte box version, data).asInstanceOf[EVT]
  }
  def decode(name: String, data: SF) = {
    val decoderMethod = decoderMethods.getOrElse(name, decoderNotFound(name, noVersion = true))
    decoderMethod.invoke(this, data).asInstanceOf[EVT]
  }
}
