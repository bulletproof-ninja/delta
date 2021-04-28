package delta.read.impl

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import delta.{ Revision, Tick }
import delta.process.StreamProcessStore

private[impl] trait ProcessStoreSupport[ID, SID, InUse >: Null, AtRest, U] {
  rm: EventSourceReadModel[ID, SID, _, InUse, AtRest] =>

  protected def processContext(id: SID): ExecutionContext
  protected def processStore: StreamProcessStore[StreamId, AtRest, U]
  def name = processStore.name

  private type Update = delta.process.Update[U]

  protected def readAndUpdate(id: ID, minRevision: Revision = -1, minTick: Tick = Long.MinValue)(
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
