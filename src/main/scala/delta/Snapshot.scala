package delta

case class Snapshot[T](
  content: T,
  revision: Int,
  tick: Long)
