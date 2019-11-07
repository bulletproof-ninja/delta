package delta.process

import scala.concurrent._
import scuff.StreamConsumer
import scala.reflect.ClassTag
import java.util.concurrent.ScheduledExecutorService

/**
 * Extension of [[delta.process.PersistentMonotonicConsumer]] with support
 * for join state (cross stream state).
 */
abstract class PersistentMonotonicJoinConsumer[ID, EVT: ClassTag, S >: Null](
    processStore: StreamProcessStore[ID, S],
    replayPersistenceContext: ExecutionContext,
    scheduler: ScheduledExecutorService)
  extends PersistentMonotonicConsumer(processStore, replayPersistenceContext, scheduler)
  with JoinState[ID, EVT, S] {

  override protected def replayProcessor(es: EventSource): StreamConsumer[TXN, Future[ReplayResult]] =
    new ReplayProcessor with MonotonicJoinState[ID, EVT, S] {

      def processStream(txn: TXN, currState: Option[S]) =
        PersistentMonotonicJoinConsumer.this.processStream(txn, currState)

      def prepareJoin(
          streamId: ID, streamRevision: Int, tick: Long, metadata: Map[String, String])(
          evt: EVT): Map[ID, Processor] =
        PersistentMonotonicJoinConsumer.this.prepareJoin(streamId, streamRevision, tick, metadata)(evt)

    }

  override protected def liveProcessor(es: EventSource, replayResult: Option[ReplayResult]): TXN => Any =
    new LiveProcessor(es) with MonotonicJoinState[ID, EVT, S] {

      def processStream(txn: TXN, currState: Option[S]) =
        PersistentMonotonicJoinConsumer.this.processStream(txn, currState)

      def prepareJoin(
          streamId: ID, streamRevision: Int, tick: Long, metadata: Map[String, String])(
          evt: EVT): Map[ID, Processor] =
        PersistentMonotonicJoinConsumer.this.prepareJoin(streamId, streamRevision, tick, metadata)(evt)

  }

}
