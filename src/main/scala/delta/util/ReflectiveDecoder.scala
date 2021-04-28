package delta.util

import scala.reflect.{ ClassTag, classTag, NameTransformer }

import java.util.{ HashMap => JMap }
import java.lang.reflect.{ Method, InvocationTargetException }

import delta._

object ReflectiveDecoder {
  sealed trait DecoderMethodMatch
  case object MatchOnReturnType extends DecoderMethodMatch
  case object MatchOnMethodName extends DecoderMethodMatch

  def matchOnMethodName = MatchOnMethodName
  def matchOnReturnType = MatchOnReturnType
}

/**
 * Partial [[delta.EventFormat]] implementation that will look for
 * event decoding methods, which must match the supplied decoder signature,
 * i.e. a single argument method taking [[delta.EventFormat.Encoded]] instance
 * and returning an event.
 *
 * This is a convenience class to allow encoding and decoding of events
 * to be colocated, without having to rely on symmetric traits.
 * Encoder methods and decoder methods are matched upon instantiation, thus
 * will fail-fast at startup.
 */
abstract class ReflectiveDecoder[EVT: ClassTag, SF <: AnyRef: ClassTag] private (
  decoderMatch: ReflectiveDecoder.DecoderMethodMatch,
  exclusiveChannel: Option[Channel])
extends EventFormat[EVT, SF] {

  import ReflectiveDecoder._

  protected def this() =
    this(ReflectiveDecoder.MatchOnReturnType, None)
  protected def this(exclusiveChannel: Channel) =
    this(ReflectiveDecoder.MatchOnReturnType, Option(exclusiveChannel))
  protected def this(decoderSig: ReflectiveDecoder.DecoderMethodMatch) =
    this(decoderSig, None)
  protected def this(decoderSig: ReflectiveDecoder.DecoderMethodMatch, exclusiveChannel: Channel) =
    this(decoderSig, Option(exclusiveChannel))

  private[this] val getEventName: (Method => String) = {
    decoderMatch match {
      case MatchOnMethodName => (method) => NameTransformer decode method.getName
      case MatchOnReturnType => (method) => signature(method.getReturnType.asInstanceOf[Class[EVT]]).name
    }
  }

  private final class Decoder(method: Method) {
    def apply(encoded: Encoded): EVT = {
      try method.invoke(ReflectiveDecoder.this, encoded).asInstanceOf[EVT] catch {
        case ite: InvocationTargetException =>
          throw ite.getCause
      }
    }
    override def toString() = method.toString()
  }

  private def tryDecode(encoded: Encoded): EVT = {
    decoders.get(encoded.name) match {
      case null =>
        val methodName = if (decoderMatch == MatchOnMethodName) s"`${encoded.name}`" else "decodeMethod"
        val returnType = encoderEvents.get(encoded.name).map(_.getSimpleName) getOrElse s"_ <: ${classTag[EVT].runtimeClass.getSimpleName}"
        val arg = classOf[Encoded].getSimpleName
        val signature = s"def $methodName($arg): $returnType"
        val message = s"""No decoding method found for event "${encoded.name}". Expected signature: $signature"""
        throw new IllegalStateException(message)
      case decoder => decoder(encoded)
    }
  }
  private def encoderEvents: Map[String, EventClass] =
    getClass.getMethods.flatMap { m =>
      val EvtClass = classTag[EVT].runtimeClass
      val FmtClass = classTag[SF].runtimeClass
      val argTypes = m.getParameterTypes
      if (argTypes.length == 1 &&
        EvtClass.isAssignableFrom(argTypes(0)) &&
        EvtClass != argTypes(0) &&
        FmtClass.isAssignableFrom(m.getReturnType)) {
        val evtType = argTypes(0).asInstanceOf[Class[EVT]]
        Some(signature(evtType).name -> evtType)
      } else None
    }.toMap

  private lazy val decoders: JMap[String, Decoder] = {
    val EvtClass = classTag[EVT].runtimeClass
    val decoderMethodsWithName: List[(String, Method)] = getClass.getMethods.toList.filter { m =>
      val parmTypes = m.getParameterTypes
      parmTypes.length == 1 &&
        classOf[Encoded].isAssignableFrom(parmTypes(0)) &&
        EvtClass.isAssignableFrom(m.getReturnType)
    }.map(m => getEventName(m) -> m)
    // When using return type, there can be more than one decoder. Verify there's not.
    if (decoderMatch == MatchOnReturnType) {
      decoderMethodsWithName.groupBy(_._1).toSeq.filter(_._2.size > 1).headOption.foreach {
        case (evtName, methods) =>
          val nlIndent = "\n\t"
          val methodsString = methods.mkString(nlIndent, nlIndent, "")
          throw new IllegalStateException(
            s"""Event "$evtName" has ambiguous decoding by the following methods:$methodsString""")
      }
    }
    val decodersByName = decoderMethodsWithName.foldLeft(new JMap[String, Decoder]) {
      case (map, (name, method)) => map.put(name, new Decoder(method)); map
    }

    val missingDecoders = encoderEvents.keys.filterNot(decodersByName.containsKey)
    if (missingDecoders.nonEmpty) {
      val missing = missingDecoders.mkString("[", ", ", "]")
      val methodName = if (decoderMatch == MatchOnMethodName) "`<event-name>`" else "decodeMethod"
      val returnType = s"${classTag[EVT].runtimeClass.getSimpleName}"
      val arg = classOf[Encoded].getSimpleName
      val signature = s"def $methodName($arg): $returnType"
      throw new IllegalStateException(s"No decoder methods found in ${getClass} for events $missing. Expected signature: $signature")
    }
    decodersByName
  }

  private[this] val channelOrNull: Channel = exclusiveChannel getOrElse null.asInstanceOf[Channel]
  @inline
  private def verifyChannel(encoded: Encoded): Unit = {
    if (channelOrNull != null && channelOrNull != encoded.channel) {
      throw new IllegalStateException(s"${classOf[ReflectiveDecoder[_, _]].getName} instance ${getClass.getName}, dedicated to events from ${channelOrNull}, is being asked to decode event '${encoded.name}' from channel '${encoded.channel}'")
    }
  }

  def decode(encoded: Encoded): EVT = {
    verifyChannel(encoded)
    tryDecode(encoded)
  }

}
