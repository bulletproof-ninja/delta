package delta.read.impl

import scala.concurrent._, duration._
import delta.read._

/**
 * Read model that relies on `snapshotStore` being pre-built
 * and continuously updated by _another thread or process_.
 * The subscription implementation is left out, but can be
 * easily augmented by adding [[delta.read.MessageHubSupport]]
 * to an instance of this class.
 * @tparam ID The specific identifier type
 * @tparam ESID The more general EventSource identifier type
 * @tparam SS The snapshot store state type
 * @tparam S The read model type, often the same as `SS`
 */
abstract class PrebuiltReadModel[ID, S, U](
  defaultReadTimeout: FiniteDuration = DefaultReadTimeout)
extends BasicReadModel[ID, S]
with SubscriptionSupport[ID, S, U] {

  protected def readAgain(id: ID, minRevision: Int, minTick: Long)(
      implicit
      ec: ExecutionContext): Future[Option[Snapshot]] =
    readSnapshot(id)

  def read(id: ID, minTick: Long)(
      implicit
      ec: ExecutionContext): Future[Snapshot] =
    read(id, minTick, defaultReadTimeout)

  def read(id: ID, minRevision: Int)(
      implicit
      ec: ExecutionContext): Future[Snapshot] =
    read(id, minRevision, defaultReadTimeout)

}
