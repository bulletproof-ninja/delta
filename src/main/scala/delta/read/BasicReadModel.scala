package delta.read

import scala.concurrent.{ Future, ExecutionContext }

/**
 * Basic read-model. May be either consistent
 * or eventually consistent, depending on
 * implementation.
 */
trait BasicReadModel[ID, S] {

  type Snapshot = delta.Snapshot[S]

  protected def readSnapshot(id: ID)(
      implicit
      ec: ExecutionContext): Future[Option[Snapshot]]

  /**
   * Read latest snapshot. This is intended
   * to get *some* revision fast. This
   * may, or may not, be the latest revision.
   *
   * @return Latest accessible snapshot, or [[delta.read.UnknownIdRequested]] if unknown id
   */
  def read(id: ID)(
      implicit
      ec: ExecutionContext): Future[Snapshot] =
    readSnapshot(id).map {
      verify(id, _)
    }

  /**
   * Read snapshot, ensuring it's at at least the given tick.
   * @param id The lookup identifier
   * @param minTick The minimum tick of snapshot
   * @return Snapshot >= `minTick` or [[delta.read.UnknownTickRequested]]
   */
  def read(id: ID, minTick: Long)(
      implicit
      ec: ExecutionContext): Future[Snapshot]

  /**
   * Read snapshot, ensuring it's at least the given revision,
   * waiting the default timeout if current revision is stale.
   * @param id The lookup identifier
   * @param minRevision The minimum revision of snapshot
   * @return Snapshot >= `minRevision` or [[delta.read.UnknownRevisionRequested]]
   */
  def read(id: ID, minRevision: Int)(
      implicit
      ec: ExecutionContext): Future[Snapshot]

}
