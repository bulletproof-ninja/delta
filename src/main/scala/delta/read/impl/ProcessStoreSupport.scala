package delta.read.impl

import scala.concurrent.ExecutionContext
import delta.process.StreamProcessStore
import scala.concurrent.Future

private[impl] trait ProcessStoreSupport[ID, ESID, S >: Null] {
  rm: EventSourceReadModel[ID, ESID, S, _] =>

  protected def processContext(id: ESID): ExecutionContext
  protected def processStore: StreamProcessStore[ESID, S]
  protected def idConv(id: ID): ESID

  private type SnapshotUpdate = delta.process.SnapshotUpdate[S]

  protected def readAndUpdate(id: ID, minRev: Int = 0, minTick: Long = Long.MinValue)(implicit ec: ExecutionContext): Future[Option[Snapshot]] = {
    val esid: ESID = idConv(id)
    val future: Future[(Option[SnapshotUpdate], _)] =
      processStore.upsert(esid) { existing =>
        val goodEnough = existing.exists { snapshot =>
          snapshot.tick >= minTick && snapshot.revision >= minRev
        }
        val latestSnapshot: Future[Option[Snapshot]] =
          if (goodEnough) Future successful existing
          else replayToComplete(existing, esid)
        latestSnapshot.map(_ -> (()))
      }(processContext(esid))

    future.map(_._1.map(_.snapshot))
  }

}
