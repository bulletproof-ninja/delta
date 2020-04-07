package delta.util

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal
import scuff.concurrent.Threads.PiggyBack
import delta.write.Repository
import delta.write.ImmutableEntity
import delta.write.Metadata

/**
  * [[delta.write.Repository]] wrapper for non-Event-source
  * repositories, while still publishing events.
  */
abstract class PublishingRepository[ID, T <: AnyRef, EVT](
    val impl: Repository[ID, T] with ImmutableEntity[T],
    publishCtx: ExecutionContext)
  extends Repository[ID, (T, List[EVT])] with ImmutableEntity[(T, List[EVT])] {

  final type E = (T, List[EVT])

  protected def publish(id: ID, revision: Int, events: List[EVT], metadata: Metadata): Unit

  private def publishEvents(id: ID, revision: Int, events: List[EVT], metadata: Metadata): Unit = {
    if (events.nonEmpty) try publish(id, revision, events, metadata) catch {
      case NonFatal(e) => publishCtx.reportFailure(e)
    }
  }

  def exists(id: ID): Future[Option[Int]] = impl.exists(id)
  def load(id: ID): Future[(E, Int)] = impl.load(id).map {
    case (entity, rev) => entity -> Nil -> rev
  }(PiggyBack)

  protected def update[R](
      expectedRevision: Option[Int], id: ID,
      updateThunk: (E, Int) => Future[UT[R]])(
      implicit
      metadata: Metadata): Future[impl.UM[R]] = {
    @volatile var toPublish: List[EVT] = Nil
    val updated = impl.update(id, expectedRevision) {
      case (e, rev) =>
        updateThunk(e -> Nil, rev).map {
          case (result, events) =>
            toPublish = events
            result
        }(PiggyBack)
    }
    updated.foreach {
      case rev => publishEvents(id, rev, toPublish, metadata)
    }(publishCtx)
    updated
  }

  def insert(id: => ID, content: E)(
      implicit
      metadata: Metadata): Future[ID] = {
    val (state, events) = content
    val inserted = impl.insert(id, state)
    inserted.foreach { id =>
      publishEvents(id, 0, events, metadata)
    }(publishCtx)
    inserted
  }

}
