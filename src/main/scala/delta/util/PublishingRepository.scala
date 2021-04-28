package delta.util

import delta._
import delta.write._

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal
import scuff.concurrent.Threads.PiggyBack
import scala.concurrent.Promise

/**
  * [[delta.write.Repository]] wrapper for non-Event-source
  * repositories, while still publishing events.
  */
abstract class PublishingRepository[ID, T <: AnyRef, EVT](
  val impl: Repository[ID, T] with ImmutableEntity { type Loaded = (T, Revision) },
  publishCtx: ExecutionContext)
extends Repository[ID, (T, List[EVT])]
with ImmutableEntity {

  implicit protected def ec = publishCtx
  protected def publish(id: ID, revision: Revision, events: List[EVT], metadata: Metadata): Unit

  private def publishEvents(id: ID, revision: Revision, events: List[EVT], metadata: Metadata): Unit = {
    if (events.nonEmpty) try publish(id, revision, events, metadata) catch {
      case NonFatal(e) => publishCtx.reportFailure(e)
    }
  }

  type Loaded = impl.Loaded
  def revision(loaded: Loaded): Int = loaded._2

  def exists(id: ID): Future[Option[Revision]] = impl.exists(id)
  def load(id: ID): Future[Loaded] = impl.load(id)

  protected def update(
      updateThunk: Loaded => Future[UpdateReturn],
      id: ID, expectedRevision: Option[Revision])
      : Future[Revision] = {
    val toPublish = Promise[(List[EVT], Metadata)]()
    val updated = impl.update(id, expectedRevision) { loaded =>
      updateThunk(loaded).map {
        case ((result, events), metadata) =>
          toPublish success events -> metadata
          result -> metadata
      }(PiggyBack)
    }
    for {
      rev <- updated
      (events, metadata) <- toPublish.future
    } {
      publishEvents(id, rev, events, metadata)
    }
    updated
  }

  def insert(id: => ID, entity: Entity)(
      implicit
      metadata: Metadata): Future[ID] = {
    val (state, events) = entity
    val inserted = impl.insert(id, state)
    inserted.foreach(publishEvents(_, 0, events, metadata))(publishCtx)
    inserted
  }

}
