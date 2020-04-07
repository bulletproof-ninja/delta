package delta.read

import delta.SnapshotReader
import scala.concurrent._

trait SnapshotReaderSupport[ID, S]
extends StreamId[ID] {
  rm: ReadModel[ID, S] =>

  protected def snapshotReader: SnapshotReader[StreamId, _ >: S]

  protected def readSnapshot(id: ID)(
      implicit
      ec: ExecutionContext): Future[Option[Snapshot]] = {

    val future: Future[Option[delta.Snapshot[_ >: S]]] = snapshotReader read StreamId(id)

    // Snapshot store might contain other types,
    // but the id is expected to match the type
    future.asInstanceOf[Future[Option[Snapshot]]]

  }

}
