package delta

import scuff.Codec

trait EventCodec[EVT, SF] {

  type EventClass = Class[_ <: EVT]

  final def name(evt: EVT): String = signature(evt.getClass)._1
  final def version(evt: EVT): Byte = signature(evt.getClass)._2
  final def signature(evt: EVT): (String, Byte) = signature(evt.getClass)
  final def signature(cls: EventClass): (String, Byte) = signatures.get(cls)
  final def name(cls: EventClass): String = signature(cls)._1
  final def version(cls: EventClass): Byte = signature(cls)._2
  private[this] val signatures = new ClassValue[(String, Byte)] {
    def computeValue(cls: Class[_]) = {
      val evtCls = cls.asInstanceOf[EventClass]
      nameOf(evtCls) -> versionOf(evtCls)
    }
  }

  /** Unique name of event. */
  protected def nameOf(cls: EventClass): String
  /** Event version number. Must be strictly > 0. */
  protected def versionOf(cls: EventClass): Byte

  /** Encode event data. */
  def encode(evt: EVT): SF
  /** Decode to event instance. */
  def decode(name: String, version: Byte, data: SF): EVT
}

/**
  * The [[EventCodec]] defaults to
  * versioned event types to enable multiple
  * versions of an event with the same name.
  * If that's not desired, add this trait.
  */
trait NoVersioning[EVT, SF] {
  codec: EventCodec[EVT, SF] =>

  final def versionOf(evt: EventClass) = NoVersioning.NoVersion

  override final def decode(name: String, version: Byte, data: SF): EVT = {
    if (version != NoVersioning.NoVersion) {
      throw new IllegalStateException(s"""Event "$name" is not versioned, yet version $version was passed""")
    }
    decode(name, data)
  }
  def decode(name: String, data: SF): EVT

}
private[delta] object NoVersioning {
  final val NoVersion: Byte = 0
}

class EventCodecAdapter[EVT, A, B](
    fmtCodec: Codec[B, A])(
    implicit evtCodec: EventCodec[EVT, B])
  extends EventCodec[EVT, A] {

  def nameOf(cls: EventClass) = evtCodec name cls
  def versionOf(cls: EventClass) = evtCodec version cls

  def encode(evt: EVT): A = fmtCodec encode evtCodec.encode(evt)
  def decode(name: String, version: Byte, data: A): EVT = evtCodec.decode(name, version, fmtCodec decode data)

}
