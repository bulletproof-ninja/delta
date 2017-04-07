package delta

case class Snapshot[+Content](
  content: Content,
  revision: Int,
  tick: Long)
