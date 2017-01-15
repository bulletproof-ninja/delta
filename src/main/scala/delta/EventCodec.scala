package delta

trait EventCodec[EVT, SF] {

  type EventClass = Class[_ <: EVT]

  private[delta] def name(evt: EVT): String = name(evt.getClass)
  private[delta] def version(evt: EVT): Byte = version(evt.getClass)

  /** Unique name of event. */
  def name(cls: EventClass): String
  /** Event version number (> 0). */
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

  private[this] final val NoVersion = 0: Byte

  final def version(evt: EventClass) = NoVersion
  override final def version(evt: EVT) = NoVersion

  override final def decode(name: String, version: Byte, data: SF): EVT = {
    if (version != NoVersion) {
      throw new IllegalStateException(s"""Event "$name" is not versioned, yet version $version was passed""")
    }
    decode(name, data)
  }
  def decode(name: String, data: SF): EVT

}
