package delta.util

import delta._
import delta.write._

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal
import scuff.concurrent.Threads.PiggyBack

/**
  * [[delta.write.Repository]] wrapper for non-Event-source
  * repositories, while still publishing events.
  */
abstract class PublishingRepository[ID, T <: AnyRef, EVT](
  val impl: Repository[ID, T] with ImmutableEntity { type Loaded = (T, Revision) },
  publishCtx: ExecutionContext)
extends Repository[ID, (T, List[EVT])]
with ImmutableEntity {

  protected def publish(id: ID, revision: Revision, events: List[EVT], metadata: Metadata): Unit

  private def publishEvents(id: ID, revision: Revision, events: List[EVT], metadata: Metadata): Unit = {
    if (events.nonEmpty) try publish(id, revision, events, metadata) catch {
      case NonFatal(e) => publishCtx.reportFailure(e)
    }
  }

  protected type Loaded = impl.Loaded
  def revision(loaded: Loaded): Int = loaded._2

  def exists(id: ID): Future[Option[Revision]] = impl.exists(id)
  def load(id: ID): Future[Loaded] = impl.load(id)

  protected def update[R](
      updateThunk: Loaded => Future[UT[R]],
      id: ID, causalTick: Tick, expectedRevision: Option[Revision])(
      implicit
      metadata: Metadata): Future[UM[R]] = {
    @volatile var toPublish: List[EVT] = Nil
    val updated = impl.update(id, expectedRevision, causalTick) { loaded =>
      updateThunk(loaded).map {
        case (result, events) =>
          toPublish = events
          result
      }(PiggyBack)
    }
    updated.foreach(publishEvents(id, _, toPublish, metadata))(publishCtx)
    updated
  }

  def insert(id: => ID, entity: Entity, causalTick: Tick)(
      implicit
      metadata: Metadata): Future[ID] = {
    val (state, events) = entity
    val inserted = impl.insert(id, state, causalTick)
    inserted.foreach(publishEvents(_, 0, events, metadata))(publishCtx)
    inserted
  }

}
