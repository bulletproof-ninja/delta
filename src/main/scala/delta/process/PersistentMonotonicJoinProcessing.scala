package delta.process

import scala.reflect.ClassTag
import scala.concurrent.ExecutionContext

/**
 * Extension of [[delta.process.PersistentMonotonicConsumer]] with support
 * for join state (cross stream state).
 */
abstract class PersistentMonotonicJoinProcessing[SID, EVT: ClassTag, S >: Null, U](
  implicit ec: ExecutionContext)
extends PersistentMonotonicProcessing[SID, EVT, S, U]
with JoinState[SID, EVT, S] {

  override protected def replayProcessor(es: EventSource) =
    new ReplayConsumer with MonotonicJoinState[SID, EVT, S, U] {

      def processStream(tx: Transaction, currState: Option[S]) =
        PersistentMonotonicJoinProcessing.this.processStream(tx, currState)

      def prepareJoin(
          streamId: SID, streamRevision: Revision, tick: Tick, metadata: Map[String, String])(
          evt: EVT): Map[SID, Processor] =
        PersistentMonotonicJoinProcessing.this.prepareJoin(streamId, streamRevision, tick, metadata)(evt)

    }

  override protected def liveProcessor(es: EventSource) =
    new LiveConsumer(es) with MonotonicJoinState[SID, EVT, S, U] {

      def processStream(tx: Transaction, currState: Option[S]) =
        PersistentMonotonicJoinProcessing.this.processStream(tx, currState)

      def prepareJoin(
          streamId: SID, streamRevision: Revision, tick: Tick, metadata: Map[String, String])(
          evt: EVT): Map[SID, Processor] =
        PersistentMonotonicJoinProcessing.this.prepareJoin(streamId, streamRevision, tick, metadata)(evt)

  }

}
