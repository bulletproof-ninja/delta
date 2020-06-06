package delta.read.impl

import delta._
import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.ClassTag
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
 * generally slower than [[StatelessReadModel]] for
 * cheap streams, where "cheap" depends on a lot
 * of factors, e.g. stream revision size, cost of computing state, etc.
 * Unless state is required to be kept in an external store, or there
 * is no messaging infrastructure available, and it's cheap to generate,
 * consider using [[StatelessReadModel]] instead.
 * @note *Cannot* be used for state derived from joined streams.
 * @tparam ID The id type
 * @tparam SID The stream id type
 * @tparam EVT The event type used for producing state
 * @tparam Work The state type optimized for mutation
 * @tparam Stored The state type optimized for storage/reading
 * @tparam U The update type
 */
abstract class StatefulReadModel[ID, SID, EVT: ClassTag, Work >: Null: ClassTag, Stored, U](
  es: EventSource[SID, _ >: EVT])(
  implicit
  stateCodec: AsyncCodec[Work, Stored],
  idConv: ID => SID)
extends EventSourceReadModel[ID, SID, EVT, Work, Stored](es)
with ProcessStoreSupport[ID, SID, Work, Stored, U] {

  protected def readSnapshot(id: ID)(
      implicit ec: ExecutionContext): Future[Option[Snapshot]] =
    readAndUpdate(id)

  def read(id: ID, minRevision: Revision)(implicit ec: ExecutionContext): Future[Snapshot] =
    readAndUpdate(id, minRevision).map {
      verifySnapshot(id, _, minRevision = minRevision)
    }

  def read(id: ID, minTick: Tick)(implicit ec: ExecutionContext): Future[Snapshot] = {
    readAndUpdate(id, minTick = minTick).map {
      verifySnapshot(id, _, minTick = minTick)
    }
  }

}

/**
  * A parametrically simpler version of [[delta.read.impl.StatefulReadModel]],
  * where the various state types are identical (common).
  * @tparam ID The id type
  * @tparam SID The stream id type
  * @tparam EVT The event type used for producing state
  * @tparam S The state type
  */
abstract class SimpleStatefulReadModel[ID, SID, EVT: ClassTag, S >: Null: ClassTag](
  es: EventSource[SID, _ >: EVT])(
  implicit
  idConv: ID => SID)
extends StatefulReadModel[ID, SID, EVT, S, S, S](es) {

  protected val stateCodec = AsyncCodec.noop[S]
  protected def stateCodecContext = Threads.PiggyBack

}
