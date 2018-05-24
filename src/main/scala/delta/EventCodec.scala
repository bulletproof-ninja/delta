package delta

import scuff.Codec

trait EventCodec[EVT, SF] {

  type EventClass = Class[_ <: EVT]

  final def name(evt: EVT): String = signature(evt.getClass)._1
  final def version(evt: EVT): Byte = signature(evt.getClass)._2
  final def signature(evt: EVT): (String, Byte) = signatures.get(evt.getClass)
  final def signature(cls: EventClass): (String, Byte) = signatures.get(cls)
  final def name(cls: EventClass): String = signature(cls)._1
  final def version(cls: EventClass): Byte = signature(cls)._2
  private[this] val signatures = new ClassValue[(String, Byte)] {
    def computeValue(cls: Class[_]) = {
      val evtCls = cls.asInstanceOf[EventClass]
      (getName(evtCls), getVersion(evtCls))
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
  /**
    * Decode to event data.
    * @param chnanel Channel
    * @param name Event name
    * @param version Event version
    * @param data Event serialization data
    * @return The deserialized event
    */
  def decode(channel: String, name: String, version: Byte, data: SF): EVT

}

/**
  * The [[EventCodec]] defaults to
  * versioned event types to enable multiple
  * versions of an event with the same name.
  * If that's not desired, add this trait.
  */
trait NoVersioning[EVT, SF] {
  codec: EventCodec[EVT, SF] =>

  protected final def getVersion(evt: EventClass) = NoVersioning.NoVersion

  override final def decode(channel: String, name: String, version: Byte, data: SF): EVT = {
    if (version != NoVersioning.NoVersion) {
      throw new IllegalStateException(s"""Event "$name" is not versioned, yet version $version was passed""")
    }
    decode(channel, name, data)
  }
  def decode(channel: String, name: String, data: SF): EVT

}
private[delta] object NoVersioning {
  final val NoVersion: Byte = -1
}

class EventCodecAdapter[EVT, A, B](
    fmtCodec: Codec[B, A])(
    implicit evtCodec: EventCodec[EVT, B])
  extends EventCodec[EVT, A] {

  protected def getName(cls: EventClass) = evtCodec name cls
  protected def getVersion(cls: EventClass) = evtCodec version cls

  def encode(evt: EVT): A = fmtCodec encode evtCodec.encode(evt)
  def decode(channel: String, eventName: String, eventVersion: Byte, eventData: A): EVT =
    evtCodec.decode(channel, eventName, eventVersion, fmtCodec decode eventData)

}
