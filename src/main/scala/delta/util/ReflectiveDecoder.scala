package delta.util

import scala.reflect.{ ClassTag, classTag }
import java.lang.reflect.Method
import delta.EventCodec
import scala.compat.Platform
import delta.NoVersioning
import java.util.{ HashMap => JMap }
import scala.reflect.NameTransformer

/**
  * Will look for event decode methods, which must match
  * the following signature:
  * For versioned events (default), the method must have two
  * arguments, 1. `Byte`, 2. `SF` (the encoding format) and
  * for `NoVersioning` events, the method must take a single
  * argument, `SF`.
  */
abstract class ReflectiveDecoder[EVT: ClassTag, SF <: Object: ClassTag] private (channel: Option[String])
  extends EventCodec[EVT, SF] {

  protected def this() = this(None)
  protected def this(channel: String) = this(Option(channel))

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
    if (isMethodNameEventName) NameTransformer decode method.getName
    else this name method.getReturnType.asInstanceOf[Class[EVT]]

  private def decoder(evtName: String, data: SF, version: Byte = NoVersioning.NoVersion): EVT = {
    val isVersioned = version != NoVersioning.NoVersion
    decoderMethods.get(evtName) match {
      case null =>
        val versionArg = if (isVersioned) "version: Byte, " else ""
        val methodName = if (isMethodNameEventName) evtName else "someMethodName"
        val returnType = encoderEvents.get(evtName).map(_.getSimpleName) getOrElse s"_ <: ${classTag[EVT].runtimeClass.getSimpleName}"
        val signature = s"def $methodName(${versionArg}data: ${classTag[SF].runtimeClass.getName}): $returnType"
        val message = s"""No decoding method found for event "$evtName". Must match the following signature: $signature"""
        throw new IllegalStateException(message)
      case method => {
        if (isVersioned) method.invoke(this, Byte box version, data)
        else method.invoke(this, data)
      }.asInstanceOf[EVT]
    }
  }
  private def encoderEvents =
    getClass.getMethods.flatMap { m =>
      val EvtClass = classTag[EVT].runtimeClass
      val FmtClass = classTag[SF].runtimeClass
      val argTypes = m.getParameterTypes
      if (argTypes.length == 1 &&
        EvtClass.isAssignableFrom(argTypes(0)) &&
        EvtClass != argTypes(0) &&
        FmtClass.isAssignableFrom(m.getReturnType)) {
        val evtType = argTypes(0).asInstanceOf[Class[EVT]]
        Some(this.name(evtType) -> evtType)
      } else None
    }.toMap

  private lazy val decoderMethods: JMap[String, Method] = {
    val noVersion = this.isInstanceOf[NoVersioning[_, _]]
    val argCount = if (noVersion) 1 else 2
    val ByteClass = classOf[Byte]
    val EvtClass = classTag[EVT].runtimeClass
    val FmtClass = classTag[SF].runtimeClass
    val decoderMethodsWithName = getClass.getMethods.filter { m =>
      val argTypes = m.getParameterTypes
      argTypes.length == argCount &&
        argTypes(argCount - 1).isAssignableFrom(FmtClass) &&
        (noVersion || argTypes(0) == ByteClass) &&
        EvtClass.isAssignableFrom(m.getReturnType)
    }.map(m => eventName(m) -> m)
    // When using return type, there can be more than one decoder. Verify there's not.
    if (!isMethodNameEventName) {
      decoderMethodsWithName.groupBy(_._1).toSeq.filter(_._2.size > 1).headOption.foreach {
        case (evtName, methods) =>
          val nlIndent = Platform.EOL + "\t"
          val methodsString = methods.mkString(nlIndent, nlIndent, "")
          throw new IllegalStateException(
            s"""Event "$evtName" has ambiguous decoding by the following methods:$methodsString""")
      }
    }
    val decoderMethodsByName = decoderMethodsWithName.foldLeft(new JMap[String, Method]) {
      case (map, (name, method)) => map.put(name, method); map
    }
    assert(decoderMethodsByName.size == decoderMethodsWithName.size)
    val missingDecoders = encoderEvents.keys.filterNot(decoderMethodsByName.containsKey)
    if (missingDecoders.nonEmpty) {
      val missing = missingDecoders.mkString("[", ", ", "]")
      throw new IllegalStateException(s"No decoder methods found in ${getClass} for events $missing")
    }
    decoderMethodsByName
  }

  private[this] val channelOrNull = channel.orNull
  @inline
  private def verifyChannel(channel: String, event: String): Unit = {
    if (channelOrNull != null && channelOrNull != channel) {
      throw new IllegalStateException(s"${classOf[ReflectiveDecoder[_, _]].getName} instance ${getClass.getName}, dedicated to events from ${channelOrNull}, is being asked to decode event '$event' from channel '$channel'")
    }
  }
  def decode(channel: String, name: String, version: Byte, data: SF): EVT = {
    verifyChannel(channel, name)
    decoder(name, data, version)
  }
  def decode(channel: String, name: String, data: SF) = {
    verifyChannel(channel, name)
    decoder(name, data)
  }

}
