package delta.process

import scala.util.Try

object ReplayCompletion {
  case class IncompleteStream[SID](id: SID, missingRevisions: Try[Range])
}

import ReplayCompletion._

/**
  * Replay completion status.
  * @param txCount Number of transactions processed
  * @param incomplete The incomplete status, if replay processing was incomplete
  */
final case class ReplayCompletion[SID](
  txCount: Long,
  incompleteStreams: List[IncompleteStream[SID]])
