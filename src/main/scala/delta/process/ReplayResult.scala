package delta.process

final case class ReplayResult[SID](
  txCount: Long, processErrors: Map[SID, Throwable])
