package ulysses

trait EventCodec[EVT, SF] {

  type ClassEVT = Class[_ <: EVT]

  def version(evt: EVT): Short = version(evt.getClass)
  def name(evt: EVT): String = name(evt.getClass)

  def version(cls: ClassEVT): Short
  def name(cls: ClassEVT): String

  def encode(evt: EVT): SF
  def decode(name: String, version: Short, data: SF): EVT

}

trait NoVersioning[EVT, SF] {
  codec: EventCodec[EVT, SF] =>

  private[this] final val NoVersion = -1

  final def version(evt: ClassEVT): Short = NoVersion
  override final def version(evt: EVT): Short = NoVersion

  override final def decode(name: String, version: Short, data: SF): EVT = {
    if (version != NoVersion) {
      throw new IllegalArgumentException(s"""Event "$name" is not versioned, yet version $version was passed""")
    }
    decode(name, data)
  }
  def decode(name: String, data: SF): EVT

}
