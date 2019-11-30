package delta.read.impl

import delta._
import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.ClassTag
import delta.read._

/**
 * Fully consistent (read-your-writes)
 * on-demand pull-only read-model.
 * Uses a [[delta.util.StreamProcessStore]] for
 * storing state and queries a [[delta.EventSource]]
 * for any new events, and updates the process store
 * if anything changed. This means that this is
 * generally slower than [[StatelessReadModel]] for
 * smaller streams, where "smaller" depends on a lot
 * of factors. Unless state is required to be kept
 * in an external store, consider using [[StatelessReadModel]]
 * instead.
 * NOTE: *Cannot* be used for state derived
 * from joined streams.
 * @tparam ID The id type
 * @tparam S The state type
 * @tparam ESID The event-source id type
 * @tparam EVT The event type used for producing state
 */
abstract class StatefulReadModel[ID, ESID, S >: Null: ClassTag, EVT: ClassTag](
    txProjector: TransactionProjector[S, EVT],
    es: EventSource[ESID, _ >: EVT])(
    implicit
    convId: ID => ESID)
  extends EventSourceReadModel[ID, ESID, S, EVT](es, txProjector)
  with ProcessStoreSupport[ID, ESID, S] {

  def this(
      projector: Projector[S, EVT])(
      es: EventSource[ESID, _ >: EVT])(
      implicit
      convId: ID => ESID) =
        this(TransactionProjector[S, EVT](projector), es)

  protected def idConv(id: ID): ESID = convId(id)

  def read(id: ID)(implicit ec: ExecutionContext): Future[Snapshot] =
    readAndUpdate(id).map {
      verifySnapshot(id, _)
    }

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
