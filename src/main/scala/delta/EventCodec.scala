package delta

import scuff.Codec

trait EventCodec[EVT, SF] {

  type EventClass = Class[_ <: EVT]

  private[delta] final def getName(evt: EVT): String = signature(evt.getClass)._1
  private[delta] final def getVersion(evt: EVT): Byte = signature(evt.getClass)._2
  private[delta] final def signature(evt: EVT): (String, Byte) = signature(evt.getClass)
  private[delta] final def signature(cls: EventClass): (String, Byte) = signatures.get(cls)
  private[delta] final def getName(cls: EventClass): String = signature(cls)._1
  private[delta] final def getVersion(cls: EventClass): Byte = signature(cls)._2
  private[this] val signatures = new ClassValue[(String, Byte)] {
    def computeValue(cls: Class[_]) = {
      val evtCls = cls.asInstanceOf[EventClass]
      name(evtCls) -> version(evtCls)
    }
  }

  /** Return unique name of event. */
  protected def name(cls: EventClass): String
  /** Return event version number. Must be strictly > 0. */
  protected def version(cls: EventClass): Byte

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

  protected final def version(evt: EventClass) = NoVersioning.NoVersion

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

  protected def name(cls: EventClass) = evtCodec getName cls
  protected def version(cls: EventClass) = evtCodec getVersion cls

  def encode(evt: EVT): A = fmtCodec encode evtCodec.encode(evt)
  def decode(channel: String, eventName: String, eventVersion: Byte, eventData: A): EVT =
    evtCodec.decode(channel, eventName, eventVersion, fmtCodec decode eventData)

}
