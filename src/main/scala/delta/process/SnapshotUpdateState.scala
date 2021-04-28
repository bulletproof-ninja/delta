package delta.process

final case class ReplayState[S](
  snapshot: delta.Snapshot[S],
  updated: Boolean = true)
