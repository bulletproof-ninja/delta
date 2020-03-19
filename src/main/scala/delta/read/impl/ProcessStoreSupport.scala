package delta.read.impl

import scala.concurrent.ExecutionContext
import delta.process.StreamProcessStore
import scala.concurrent.Future

private[impl] trait ProcessStoreSupport[ID, ESID, Work >: Null, Stored, U] {
  rm: EventSourceReadModel[ID, ESID, _, Work, Stored] =>

  protected def processContext(id: ESID): ExecutionContext
  protected def processStore: StreamProcessStore[StreamId, Stored, U]

  private type Update = delta.process.Update[U]

  protected def readAndUpdate(id: ID, minRevision: Int = -1, minTick: Long = Long.MinValue)(
      implicit
      ec: ExecutionContext): Future[Option[Snapshot]] = {

    val streamId: StreamId = StreamId(id)

    val future: Future[(Option[Update], Option[Snapshot])] =
      processStore.upsert(streamId) { existing =>
        val goodEnough = existing.exists { snapshot =>
          snapshot.tick >= minTick && snapshot.revision >= minRevision
        }
        val latestSnapshot: Future[Option[Snapshot]] =
          if (goodEnough) Future successful existing
          else replayToComplete(existing, streamId)
        latestSnapshot.map(ls => ls -> ls)
      }(processContext(streamId))

    future.map(_._2)

  }

}
