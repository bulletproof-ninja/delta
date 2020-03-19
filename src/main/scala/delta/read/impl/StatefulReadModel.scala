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
 * if anything changed. This means that this is
 * generally slower than [[StatelessReadModel]] for
 * cheap streams, where "cheap" depends on a lot
 * of factors, e.g. stream revision size, cost of computing state, etc.
 * Unless state is required to be kept in an external store, or there
 * is no messaging infrastructure available, and it's cheap to generate,
 * consider using [[StatelessReadModel]] instead.
 * NOTE: *Cannot* be used for state derived from joined streams.
 * @tparam ID The id type
 * @tparam ESID The event-source id type
 * @tparam EVT The event type used for producing state
 * @tparam Work The state type optimized for mutation
 * @tparam Stored The state type optimized for storage/reading
 */
abstract class StatefulReadModel[ID, ESID, EVT: ClassTag, Work >: Null: ClassTag, Stored, U](
  es: EventSource[ESID, _ >: EVT])(
  implicit
  idConv: ID => ESID)
extends EventSourceReadModel[ID, ESID, EVT, Work, Stored](es)
with ProcessStoreSupport[ID, ESID, Work, Stored, U] {

  protected def readSnapshot(id: ID)(
      implicit ec: ExecutionContext): Future[Option[Snapshot]] =
    readAndUpdate(id)

  def read(id: ID, minRevision: Int)(implicit ec: ExecutionContext): Future[Snapshot] =
    readAndUpdate(id, minRevision).map {
      verifySnapshot(id, _, minRevision = minRevision)
    }

  def read(id: ID, minTick: Long)(implicit ec: ExecutionContext): Future[Snapshot] = {
    readAndUpdate(id, minTick = minTick).map {
      verifySnapshot(id, _, minTick = minTick)
    }
  }

}

abstract class SimpleStatefulReadModel[ID, ESID, EVT: ClassTag, S >: Null: ClassTag](
  es: EventSource[ESID, _ >: EVT])(
  implicit
  idConv: ID => ESID)
extends StatefulReadModel[ID, ESID, EVT, S, S, S](es) {

  protected val stateCodec = AsyncCodec.noop[S]
  protected def stateCodecContext = Threads.PiggyBack


}
