package delta.read.impl

import delta._
import scala.concurrent._
import scala.reflect.ClassTag
import delta.read._

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
class StatelessReadModel[ID, ESID, S >: Null: ClassTag, EVT: ClassTag] private (
    projectorSource: Either[Map[String, String] => Projector[S, EVT], Projector[S, EVT]],
    es: EventSource[ESID, _ >: EVT])(
    implicit
    idConv: ID => ESID)
  extends EventSourceReadModel[ID, ESID, S, EVT](es, projectorSource) {

  def this(projector: Projector[S, EVT])(
      es: EventSource[ESID, _ >: EVT])(
      implicit
      idConv: ID => ESID) = this(Right(projector), es)

  def this(withMetadata: Map[String, String] => Projector[S, EVT])(
      es: EventSource[ESID, _ >: EVT])(
      implicit
      idConv: ID => ESID) = this(Left(withMetadata), es)

  def readLatest(id: ID)(implicit ec: ExecutionContext): Future[Snapshot] =
    replayToComplete(None, id).flatMap {
      verify(id, _)
    }

  def readMinRevision(id: ID, minRevision: Int)(implicit ec: ExecutionContext): Future[Snapshot] =
    readLatest(id).flatMap {
      verifyRevision(id, _, minRevision)
    }

  def readMinTick(id: ID, minTick: Long)(implicit ec: ExecutionContext): Future[Snapshot] = {
    readLatest(id).flatMap {
      verifyTick(id, _, minTick)
    }
  }

}
