package delta.read.impl

import delta._
import delta.process.AsyncCodec
import scala.concurrent._
import scala.reflect.ClassTag
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
 * @tparam Work The work state type
 * @tparam View The view model type
 */
abstract class StatelessReadModel[ID, SID, EVT: ClassTag, Work >: Null: ClassTag, View](
  protected val name: String,
  es: EventSource[SID, _ >: EVT])(
  implicit
  stateCodec: AsyncCodec[Work, View],
  idConv: ID => SID)
extends EventSourceReadModel[ID, SID, EVT, Work, View](es) {

  protected def readSnapshot(id: ID)(
      implicit ec: ExecutionContext): Future[Option[Snapshot]] =
    replayToComplete(None, id)

  def read(id: ID, minRevision: Revision)(implicit ec: ExecutionContext): Future[Snapshot] =
    read(id).map {
      verifyRevision(id, _, minRevision)
    }

  def read(id: ID, minTick: Tick)(implicit ec: ExecutionContext): Future[Snapshot] = {
    read(id).map {
      verifyTick(id, _, minTick)
    }
  }

}

/**
  * A parametrically simpler version of [[delta.read.impl.StatelessReadModel]],
  * where the various state types are identical (common).
  * @tparam ID The id type
  * @tparam SID The stream id type
  * @tparam EVT The event type used for producing state
  * @tparam S The state type
  */
abstract class SimpleStatelessReadModel[ID, SID, EVT: ClassTag, S >: Null: ClassTag](
  name: String,
  es: EventSource[SID, _ >: EVT])(
  implicit
  idConv: ID => SID)
extends StatelessReadModel[ID, SID, EVT, S, S](name, es) {

  protected val stateCodec = AsyncCodec.noop[S]
  protected def stateCodecContext = Threads.PiggyBack

}
