package delta.util

import scala.reflect.{ ClassTag, classTag }
import java.lang.reflect.Method
import delta.EventCodec
import scala.compat.Platform
import delta.NoVersioning
import java.util.{ HashMap => JMap }

/**
  * Will look for event decode methods, which must match
  * the following signature:
  * For versioned events (default), the method must have two
  * arguments, 1. `Byte`, 2. `SF` (the encoding format) and
  * for `NoVersioning` events, the method must take a single
  * argument, `SF`.
  */
abstract class ReflectiveDecoder[EVT: ClassTag, SF <: AnyRef: ClassTag] {
  codec: EventCodec[EVT, SF] =>

  /**
    * Is method name == event name?
    *
    * If `true` then it's possible to have multiple
    * methods decode to the same event. This is helpful
    * when using deprecated events that are upgraded to
    * new event type.
    * If `false` then method name is ignored, but return
    * type defines which event the method decodes.
    */
  protected def isMethodNameEventName: Boolean = false
  private def eventName(method: Method): String =
    if (isMethodNameEventName) method.getName
    else codec.name(method.getReturnType.asInstanceOf[Class[EVT]])

  private def decoder(evtName: String, data: SF, version: Byte = NoVersioning.NoVersion): EVT = {
    val isVersioned = version != NoVersioning.NoVersion
    decoderMethods.get(evtName) match {
      case null =>
        val versionArg = if (isVersioned) "version: Byte, " else ""
        val methodName = if (isMethodNameEventName) evtName else "<decoderMethod>"
        val signature = s"def $methodName(${versionArg}data: ${classTag[SF].runtimeClass.getName}): EVT"
        val message = s"""No decoding method found for event "$evtName". Must match the following signature, where EVT is a sub-type of ${classTag[EVT].runtimeClass.getName}: $signature"""
        throw new IllegalStateException(message)
      case method => {
        if (isVersioned) method.invoke(this, Byte box version, data)
        else method.invoke(this, data)
      }.asInstanceOf[EVT]
    }
  }
  private[this] lazy val decoderMethods: JMap[String, Method] = {
    val noVersion = codec.isInstanceOf[NoVersioning[_, _]]
    val argCount = if (noVersion) 1 else 2
    val ByteClass = classOf[Byte]
    val EvtClass = classTag[EVT].runtimeClass
    val FmtClass = classTag[SF].runtimeClass
    val decoderMethods = getClass.getMethods.filter { m =>
      val argTypes = m.getParameterTypes
      argTypes.length == argCount &&
        argTypes(argCount - 1).isAssignableFrom(FmtClass) &&
        (noVersion || argTypes(0) == ByteClass) &&
        EvtClass.isAssignableFrom(m.getReturnType)
    }.map(m => eventName(m) -> m)
    // When using return type, there can be more than one decoder. Verify there's not.
    if (!isMethodNameEventName) {
      decoderMethods.groupBy(_._1).toSeq.filter(_._2.size > 1).headOption.foreach {
        case (evtName, methods) =>
          val nlIndent = Platform.EOL + "\t"
          val methodsString = methods.mkString(nlIndent, nlIndent, "")
          throw new IllegalStateException(
            s"""Event "$evtName" has ambiguous decoding by the following methods:$methodsString""")
      }
    }
    val encoderEvents = getClass.getMethods.flatMap { m =>
      val argTypes = m.getParameterTypes
      if (argTypes.length == 1 &&
        EvtClass.isAssignableFrom(argTypes(0)) &&
        EvtClass != argTypes(0) &&
        FmtClass.isAssignableFrom(m.getReturnType)) {
        val evtType = argTypes(0).asInstanceOf[Class[EVT]]
        Some(codec.name(evtType))
      } else None
    }.toSet
    val decoderMethodsByName = decoderMethods.foldLeft(new JMap[String, Method]) {
      case (jmap, (name, method)) => jmap.put(name, method); jmap
    }
    assert(decoderMethodsByName.size == decoderMethods.size)
    val missingDecoders = encoderEvents.filterNot(decoderMethodsByName.containsKey)
    if (missingDecoders.nonEmpty) {
      val missing = missingDecoders.mkString("[", ", ", "]")
      throw new IllegalStateException(s"No decoder methods found in ${getClass} for events $missing")
    }
    decoderMethodsByName
  }

  def decode(name: String, version: Byte, data: SF): EVT = decoder(name, data, version)
  def decode(name: String, data: SF) = decoder(name, data)

}
