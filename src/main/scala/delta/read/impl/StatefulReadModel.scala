package delta.read.impl

import delta.Projector
import delta.EventSource
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
abstract class StatefulReadModel[ID, ESID, S >: Null: ClassTag, EVT: ClassTag] private (
    projectorSource: Either[Map[String, String] => Projector[S, EVT], Projector[S, EVT]],
    es: EventSource[ESID, _ >: EVT])(
    implicit
    convId: ID => ESID)
  extends EventSourceReadModel[ID, ESID, S, EVT](es, projectorSource)
  with ProcessStoreSupport[ID, ESID, S] {

  def this(
      projector: Projector[S, EVT])(
      es: EventSource[ESID, _ >: EVT])(
      implicit
      convId: ID => ESID) = this(Right(projector), es)

  def this(
      withMetadata: Map[String, String] => Projector[S, EVT])(
      es: EventSource[ESID, _ >: EVT])(
      implicit
      convId: ID => ESID) = this(Left(withMetadata), es)

  protected def idConv(id: ID): ESID = convId(id)

  def readLatest(id: ID)(implicit ec: ExecutionContext): Future[Snapshot] =
    readAndUpdate(id).flatMap {
      verify(id, _)
    }

  def readMinRevision(id: ID, minRevision: Int)(implicit ec: ExecutionContext): Future[Snapshot] =
    readAndUpdate(id, minRevision).flatMap {
      verifyRevision(id, _, minRevision)
    }

  def readMinTick(id: ID, minTick: Long)(implicit ec: ExecutionContext): Future[Snapshot] = {
    readAndUpdate(id, minTick = minTick).flatMap {
      verifyTick(id, _, minTick)
    }
  }

}
