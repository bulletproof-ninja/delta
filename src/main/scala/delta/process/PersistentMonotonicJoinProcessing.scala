package delta.process

import scala.reflect.ClassTag
import scala.concurrent.ExecutionContext

/**
 * Extension of [[delta.process.PersistentMonotonicProcessing]] with support
 * for join state (cross stream state).
  * @see [[delta.process.PersistentMonotonicProcessing]] for details.
 */
abstract class PersistentMonotonicJoinProcessing[SID, EVT: ClassTag, S >: Null, U]
extends PersistentMonotonicProcessing[SID, EVT, S, U]
with JoinState[SID, EVT, S] {

  protected def adHocContext: ExecutionContext

  override protected def replayProcessor(es: EventSource, config: ReplayProcessConfig) =
    new ReplayConsumer(config, adHocContext) with MonotonicJoinState[SID, EVT, S, U] {

      def processStream(tx: Transaction, currState: Option[S]) =
        PersistentMonotonicJoinProcessing.this.processStream(tx, currState)

      def prepareJoin(
          streamId: SID, streamRevision: Revision, tick: Tick, metadata: Map[String, String])(
          evt: EVT): Map[SID, Processor] =
        PersistentMonotonicJoinProcessing.this.prepareJoin(streamId, streamRevision, tick, metadata)(evt)

    }

  override protected def liveProcessor(es: EventSource, config: LiveProcessConfig) =
    new LiveConsumer(es, config) with MonotonicJoinState[SID, EVT, S, U] {

      def processStream(tx: Transaction, currState: Option[S]) =
        PersistentMonotonicJoinProcessing.this.processStream(tx, currState)

      def prepareJoin(
          streamId: SID, streamRevision: Revision, tick: Tick, metadata: Map[String, String])(
          evt: EVT): Map[SID, Processor] =
        PersistentMonotonicJoinProcessing.this.prepareJoin(streamId, streamRevision, tick, metadata)(evt)

  }

}

/**
  * Recommended super class for implementing [[delta.EventSource]]
  * consumption with cross referenced streams in different channels.
  * @see [[delta.process.PersistentMonotonicJoinProcessing]] for details.
  */
abstract class PersistentMonotonicJoinConsumer[SID, EVT: ClassTag, Work >: Null, U]
extends PersistentMonotonicJoinProcessing[SID, EVT, Work, U]
with EventSourceConsumer[SID, EVT]
