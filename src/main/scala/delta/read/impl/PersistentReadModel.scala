package delta.read.impl

import delta._
import scala.concurrent.{ ExecutionContext, Future }

import delta.process.AsyncCodec
import delta.read._
import scuff.concurrent.Threads

/**
 * Fully consistent (read-your-writes)
 * on-demand pull-only read-model.
 * Uses a [[delta.process.StreamProcessStore]] for
 * storing state and queries a [[delta.EventSource]]
 * for any new events, and updates the process store
 * if anything changed. This means that it is
 * generally slower than [[EphemeralReadModel]] for
 * cheap streams, where "cheap" depends on a lot
 * of factors, e.g. stream revision size, cost of computing state, etc.
 * Unless state is required to be kept in an external store, or there
 * is no messaging infrastructure available, and it's cheap to generate,
 * consider using [[EphemeralReadModel]] instead.
 * @note *Cannot* be used for state derived from joined streams.
 * @tparam ID The id type
 * @tparam SID The stream id type
 * @tparam EVT The event type used for producing state
 * @tparam InUse The state type optimized for mutation
 * @tparam AtRest The state type optimized for storage/reading
 * @tparam U The update type
 */
private[impl] abstract class BasePersistentReadModel[ID, SID, EVT, InUse >: Null, AtRest, U](
  es: EventSource[SID, _ >: EVT])(
  implicit
  idConv: ID => SID)
extends EventSourceReadModel[ID, SID, EVT, InUse, AtRest](es)
with ProcessStoreSupport[ID, SID, InUse, AtRest, U] {

  protected def readSnapshot(id: ID)(
      implicit ec: ExecutionContext): Future[Option[Snapshot]] =
    readAndUpdate(id)

  def read(id: ID, minRevision: Revision)(implicit ec: ExecutionContext): Future[Snapshot] =
    readAndUpdate(id, minRevision).map {
      verifySnapshot(id, _, minRevision = minRevision, name)
    }

  def read(id: ID, minTick: Tick)(implicit ec: ExecutionContext): Future[Snapshot] = {
    readAndUpdate(id, minTick = minTick).map {
      verifySnapshot(id, _, minTick = minTick, name)
    }
  }

}

/**
  * A parametrically simpler version of [[delta.read.impl.PersistentReadModel]],
  * where the various state types are identical (common).
  * @tparam ID The id type
  * @tparam SID The stream id type
  * @tparam EVT The event type used for producing state
  * @tparam S The state type
  */
abstract class PersistentReadModel[ID, SID, EVT, S >: Null](
  es: EventSource[SID, _ >: EVT])(
  implicit
  idConv: ID => SID)
extends BasePersistentReadModel[ID, SID, EVT, S, S, S](es) {

  protected def stateAugmentCodec(id: SID) = AsyncCodec.noop[S]

}
