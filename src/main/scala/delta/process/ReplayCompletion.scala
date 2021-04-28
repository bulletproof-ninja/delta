package delta.process

object ReplayCompletion {
  sealed abstract class BrokenStream[SID] {
    def id: SID
    def channel: delta.Channel
  }
  case class MissingRevisions[SID](id: SID, channel: delta.Channel, missingRevisions: Range)
  extends BrokenStream[SID]
  case class ProcessingFailure[SID](id: SID, channel: delta.Channel, failure: Throwable)
  extends BrokenStream[SID]

}

import ReplayCompletion._

/**
  * Replay completion status.
  * @param txCount Number of transactions processed
  * @param brokenStreams Collection of broken streams, if any
  */
final case class ReplayCompletion[SID](
  txCount: Long,
  brokenStreams: List[BrokenStream[SID]])
