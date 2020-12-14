package delta.read

import scala.concurrent.{ Future, ExecutionContext }

import delta.{ Revision, Tick }

/**
 * Basic read-model. May be either consistent
 * or eventually consistent, depending on
 * implementation.
 */
trait ReadModel[ID, S] {

  type Id = ID
  type Snapshot = delta.Snapshot[S]

  protected def name: String

  protected def readSnapshot(id: ID)(
      implicit
      ec: ExecutionContext): Future[Option[Snapshot]]

  /**
   * Read snapshot.
   * @param minRevision Optional minimum revision. If not provided, then
   * an arbitrary revision is provided, which may, or may not, be the latest revision.
   * @return Latest accessible snapshot, or [[delta.read.UnknownIdRequested]] if unknown id
   */
  def read(id: ID, minRevision: Option[Revision] = None)(
      implicit
      ec: ExecutionContext): Future[Snapshot] =
    minRevision match {
      case Some(minRevision) =>
        read(id, minRevision)
      case _ =>
        readSnapshot(id).map {
          verify(id, _)
        }
    }

  /**
   * Read snapshot, ensuring it's at at least the given tick.
   * @param id The lookup identifier
   * @param minTick The minimum tick of snapshot
   * @return Snapshot >= `minTick` or [[delta.read.UnknownTickRequested]]
   */
  def read(id: ID, minTick: Tick)(
      implicit
      ec: ExecutionContext): Future[Snapshot]

  /**
   * Read snapshot, ensuring it's at least the given revision,
   * waiting the default timeout if current revision is stale.
   * @param id The lookup identifier
   * @param minRevision The minimum revision of snapshot
   * @return Snapshot >= `minRevision` or [[delta.read.UnknownRevisionRequested]]
   */
  def read(id: ID, minRevision: Revision)(
      implicit
      ec: ExecutionContext): Future[Snapshot]

}
