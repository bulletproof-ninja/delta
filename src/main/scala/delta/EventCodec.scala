package delta

import scuff.Codec

trait EventCodec[EVT, SF] {

  type EventClass = Class[_ <: EVT]

  def name(evt: EVT): String = name(evt.getClass)
  def version(evt: EVT): Byte = version(evt.getClass)

  /** Unique name of event. */
  def name(cls: EventClass): String
  /** Event version number. Must be strictly > 0. */
  def version(cls: EventClass): Byte

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

  final def version(evt: EventClass) = NoVersioning.NoVersion
  override final def version(evt: EVT) = NoVersioning.NoVersion

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

  def name(cls: EventClass) = evtCodec name cls
  def version(cls: EventClass) = evtCodec version cls

  def encode(evt: EVT): A = fmtCodec encode evtCodec.encode(evt)
  def decode(name: String, version: Byte, data: A): EVT = evtCodec.decode(name, version, fmtCodec decode data)

}
