package delta.read

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait SnapshotStoreSupport[ID, SSID, SS, S >: SS] {
  rm: BasicReadModel[ID, S] =>

  protected def stateClass: Class[S]
  protected def snapshotStore: delta.SnapshotStore[SSID, SS]
  protected def idConv(id: ID): SSID

  def read(id: ID)(implicit ec: ExecutionContext): Future[delta.Snapshot[S]] = {
    snapshotStore.read(idConv(id)).map {
      case Some(snapshot) =>
        if (stateClass.isInstance(snapshot.content)) snapshot.asInstanceOf[Snapshot]
        else throw new IllegalStateException(
          s"Expected type ${stateClass.getName} for id $id, but was ${snapshot.content.getClass.getName}")
      case None => throw UnknownIdRequested(id)
    }
  }

}
