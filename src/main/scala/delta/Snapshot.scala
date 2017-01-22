package delta.cqrs

case class ReadModel[D](
  data: D,
  revision: Int,
  tick: Long)
