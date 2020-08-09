package delta.read

import delta.SnapshotReader
import scala.concurrent._
import com.github.ghik.silencer.silent

/**
  * Support use of [[delta.SnapshotReader]] as
  * backend for read model.
  * @note The [[delta.process.StreamProcessStore]] is
  * a sub-type of [[delta.SnapshotReader]] and is the
  * most common usage.
  */
trait SnapshotReaderSupport[ID, S]
extends StreamId {
  rm: ReadModel[ID, S] =>

  implicit protected def toUnit[T](any: T): Unit = ()

  protected def snapshotReader: SnapshotReader[StreamId, _ >: S]

  @silent("never used")
  protected def readSnapshot(id: ID)(
      implicit
      ec: ExecutionContext): Future[Option[Snapshot]] = {

    val future: Future[Option[delta.Snapshot[_ >: S]]] = snapshotReader read StreamId(id)

    // Snapshot store might contain other types,
    // but the id is expected to match the type
    future.asInstanceOf[Future[Option[Snapshot]]]

  }

}
