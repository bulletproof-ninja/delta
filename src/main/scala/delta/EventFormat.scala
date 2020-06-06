package delta

import scuff.Codec
import scala.annotation.implicitNotFound

/**
 * Type class for event encoding/decoding.
 * @tparam EVT Event type
 * @tparam SF Serialization format
 */
@implicitNotFound("Undefined event serialization format: ${SF}. An implicit instance of delta.EventFormat[${EVT}, ${SF}] must be in scope")
trait EventFormat[EVT, SF] {

  type Encoded = EventFormat.Encoded[SF]
  type EventClass = Class[_ <: EVT]
  final def NoVersion = EventFormat.NoVersion

  final def adapt[A](implicit adapter: Codec[SF, A]): EventFormat[EVT, A] =
    new EventFormatAdapter[EVT, A, SF](adapter, this)

  final def signature(evt: EVT): EventFormat.EventSig = signatures.get(evt.getClass)
  final def signature(cls: EventClass): EventFormat.EventSig = signatures.get(cls)
  private[this] val signatures = new ClassValue[EventFormat.EventSig] {
    def computeValue(cls: Class[_]) = {
      val evtCls = cls.asInstanceOf[EventClass]
      EventFormat.EventSig(getName(evtCls), getVersion(evtCls))
    }
  }

  /** Return unique name of event. */
  protected def getName(cls: EventClass): String
  /** Return event version number. Must be strictly > 0. */
  protected def getVersion(cls: EventClass): Byte

  /**
    * Encode event data.
    * @param evt The event
    * @return Serialized event
    */
  def encode(evt: EVT): SF

  final def decode(name: String, version: Byte, data: SF, channel: Channel, metadata: Map[String, String]): EVT =
    decode(new Encoded(name, version, data, channel, metadata))

  /**
    * Decode to event.
    * @param encoded Encoding encapsulation
    */
  def decode(encoded: Encoded): EVT

}

object EventFormat {
  /** Use this if versioning is not desired. */
  final val NoVersion: Byte = -1

  final case class EventSig(name: String, version: Byte)

  /**
   * Encoded event with transaction channel and metadata.
   * @param name Event name
   * @param version Event version
   * @param data Event data
   * @param channel Stream channel
   * @param metadata Transaction metadata
   */
  class Encoded[SF] (val name: String, _version: Byte, val data: SF, val channel: Channel, val metadata: Map[String, String]) {

    def withData[T](t: T): Encoded[T] = new Encoded(name, _version, t, channel, metadata)
    def mapData[T](f: SF => T): Encoded[T] = new Encoded(name, _version, f(data), channel, metadata)

    /**
     * Version, *if* version is supported, i.e. not `NoVersion`.
     */
    @throws[IllegalStateException]("if version is unsupported")
    def version: Byte =
      if (_version != NoVersion) _version
      else throw new IllegalStateException(s"Versioning not supported for event '$name'")

  }

}

class EventFormatAdapter[EVT, A, B](
    adapter: Codec[B, A],
    evtFmt: EventFormat[EVT, B])
  extends EventFormat[EVT, A] {

  protected def getName(cls: EventClass) = (evtFmt signature cls).name
  protected def getVersion(cls: EventClass) = (evtFmt signature cls).version

  def encode(evt: EVT): A = adapter encode evtFmt.encode(evt)
  def decode(encoded: Encoded): EVT = {
    evtFmt decode
      encoded.mapData(adapter.decode)
  }

}
