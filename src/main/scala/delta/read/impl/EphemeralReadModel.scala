package delta.read.impl

import delta._
import delta.process.AsyncCodec
import scala.concurrent._

import delta.read._
import scuff.concurrent.Threads

/**
 * Fully consistent (read-your-writes)
 * on-demand pull-only read-model.
 * Uses a [[delta.EventSource]] for reading state,
 * which is what ensures consistency, and
 * builds state on-demand, but doesn't store it.
 * @note *Cannot* be used for state derived
 * from joined streams.
 * @tparam ID The id type
 * @tparam SID The stream id type
 * @tparam EVT The event type
 * @tparam InUse The in-memory state type
 * @tparam View The view model type
 */
private[impl] abstract class BaseEphemeralReadModel[ID, SID, EVT, InUse >: Null, View](
  val name: String,
  es: EventSource[SID, _ >: EVT])(
  implicit
  idConv: ID => SID)
extends EventSourceReadModel[ID, SID, EVT, InUse, View](es) {

  protected def readSnapshot(id: ID)(
      implicit ec: ExecutionContext): Future[Option[Snapshot]] =
    replayToComplete(None, id)

  def read(id: ID, minRevision: Revision)(implicit ec: ExecutionContext): Future[Snapshot] =
    read(id).map {
      verifyRevision(id, _, minRevision, name)
    }

  def read(id: ID, minTick: Tick)(implicit ec: ExecutionContext): Future[Snapshot] = {
    read(id).map {
      verifyTick(id, _, minTick, name)
    }
  }

}

/**
  * A parametrically simpler version of [[delta.read.impl.EphemeralReadModel]],
  * where the various state types are identical (common).
  * @tparam ID The id type
  * @tparam SID The stream id type
  * @tparam EVT The event type used for producing state
  * @tparam S The state type
  */
abstract class EphemeralReadModel[ID, SID, EVT, S >: Null](
  name: String,
  es: EventSource[SID, _ >: EVT])(
  implicit
  idConv: ID => SID)
extends BaseEphemeralReadModel[ID, SID, EVT, S, S](name, es) {

  protected def stateAugmentCodec(id: SID) = AsyncCodec.noop[S]

}

abstract class FlexEphemeralReadModel[ID, SID, EVT, InUse >: Null, View](
  name: String,
  es: EventSource[SID, _ >: EVT])(
  implicit
  idConv: ID => SID)
extends BaseEphemeralReadModel[ID, SID, EVT, InUse, View](name, es)