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
 * NOTE: *Cannot* be used for state derived
 * from joined streams.
 * @tparam ID The id type
 * @tparam S The state type
 * @tparam EVT The event type used for producing state
 */
abstract class StatelessReadModel[ID, ESID, EVT: ClassTag, Work >: Null: ClassTag, View](
  es: EventSource[ESID, _ >: EVT])(
  implicit
  protected val idConv: ID => ESID)
extends EventSourceReadModel[ID, ESID, EVT, Work, View](es) {

  protected def readSnapshot(id: ID)(
      implicit ec: ExecutionContext): Future[Option[Snapshot]] =
    replayToComplete(None, id)

  def read(id: ID, minRevision: Int)(implicit ec: ExecutionContext): Future[Snapshot] =
    read(id).map {
      verifyRevision(id, _, minRevision)
    }

  def read(id: ID, minTick: Long)(implicit ec: ExecutionContext): Future[Snapshot] = {
    read(id).map {
      verifyTick(id, _, minTick)
    }
  }

}

abstract class PlainStatelessReadModel[ID, ESID, EVT: ClassTag, S >: Null: ClassTag](
  es: EventSource[ESID, _ >: EVT])(
  implicit
  idConv: ID => ESID)
extends StatelessReadModel[ID, ESID, EVT, S, S](es) {

  protected val stateCodec = AsyncCodec.noop[S]
  protected def stateCodecContext = Threads.PiggyBack

}
