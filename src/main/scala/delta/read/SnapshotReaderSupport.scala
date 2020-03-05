package delta.read

import delta.SnapshotReader
import scala.concurrent._

trait SnapshotReaderSupport[ID, S] {
  rm: BasicReadModel[ID, S] =>

  protected val snapshotReader: SnapshotReader[ID, _ >: S]

  protected def readSnapshot(id: ID)(
      implicit
      ec: ExecutionContext): Future[Option[Snapshot]] = {

    val future: Future[Option[delta.Snapshot[_ >: S]]] = snapshotReader.read(id)

    // Snapshot store might contain other types,
    // but the id is expected to match the type
    future.asInstanceOf[Future[Option[Snapshot]]]

  }

}
