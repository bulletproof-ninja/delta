package ulysses

trait EventContext[EVT, CH, SF] {

  type C[EVT] = Class[_ <: EVT]

  def name(evt: C[EVT]): String
  def version(evt: C[EVT]): Short
  def channel(evt: C[EVT]): CH

  def name(evt: EVT): String = name(evt.getClass)
  def version(evt: EVT): Short = version(evt.getClass)
  def channel(evt: EVT): CH = channel(evt.getClass)

  def encode(evt: EVT): SF
  def decode(name: String, version: Short, data: SF): EVT

}
