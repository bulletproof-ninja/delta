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
    scheduler: ScheduledExecutorService)
  extends PersistentMonotonicConsumer(processStore, scheduler)
  with JoinState[ID, EVT, S] {

  override protected def replayProcessor(es: ES): StreamConsumer[TXN, Future[ReplayResult]] =
    new ReplayProcessor with MonotonicJoinState[ID, EVT, S] {

      def join(streamId: ID, streamRevision: Int, tick: Long, metadata: Map[String, String], streamState: Option[S])(evt: EVT): Map[ID, Processor] =
        PersistentMonotonicJoinConsumer.this.join(streamId, streamRevision, tick, metadata, streamState)(evt)

    }

  override protected def liveProcessor(es: ES, replayResult: Option[ReplayResult]): TXN => Any =
    new LiveProcessor(es) with MonotonicJoinState[ID, EVT, S] {

      def join(streamId: ID, streamRevision: Int, tick: Long, metadata: Map[String, String], streamState: Option[S])(evt: EVT): Map[ID, Processor] =
        PersistentMonotonicJoinConsumer.this.join(streamId, streamRevision, tick, metadata, streamState)(evt)

  }

}
