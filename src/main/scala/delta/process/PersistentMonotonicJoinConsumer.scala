package delta.process

import scala.concurrent._
import scuff.StreamConsumer
import scala.reflect.ClassTag

/**
 * Extension of [[delta.process.PersistentMonotonicConsumer]] with support
 * for join state (cross stream state).
 */
abstract class PersistentMonotonicJoinConsumer[ID, EVT: ClassTag, S >: Null, U]
  extends PersistentMonotonicConsumer[ID, EVT, S, U]
  with JoinState[ID, EVT, S] {

  override protected def replayProcessor(es: EventSource): StreamConsumer[Transaction, Future[ReplayResult]] =
    new ReplayProcessor with MonotonicJoinState[ID, EVT, S, U] {

      def processStream(tx: Transaction, currState: Option[S]) =
        PersistentMonotonicJoinConsumer.this.processStream(tx, currState)

      def prepareJoin(
          streamId: ID, streamRevision: Int, tick: Long, metadata: Map[String, String])(
          evt: EVT): Map[ID, Processor] =
        PersistentMonotonicJoinConsumer.this.prepareJoin(streamId, streamRevision, tick, metadata)(evt)

    }

  override protected def liveProcessor(es: EventSource, replayResult: Option[ReplayResult]): Transaction => Any =
    new LiveProcessor(es) with MonotonicJoinState[ID, EVT, S, U] {

      def processStream(tx: Transaction, currState: Option[S]) =
        PersistentMonotonicJoinConsumer.this.processStream(tx, currState)

      def prepareJoin(
          streamId: ID, streamRevision: Int, tick: Long, metadata: Map[String, String])(
          evt: EVT): Map[ID, Processor] =
        PersistentMonotonicJoinConsumer.this.prepareJoin(streamId, streamRevision, tick, metadata)(evt)

  }

}
