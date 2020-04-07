package delta.write

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal

import delta.EventStore
import delta.SnapshotStore

/**
  * [[delta.write.Entity]]-based [[delta.write.Repository]] implementation.
  * @tparam ESID Event store id type
  * @tparam EVT Repository event type
  * @tparam S Repository state type
  * @tparam ID Repository (Entity) id type. Must be the same as, or translatable to, the event store id type
  * @tparam ET The entity type
  * @param entity Entity type class
  * @param exeCtx ExecutionContext for basic Future transformations
  * @param eventStore The event store implementation
  * @param ticker Ticker implementation
  * @param snapshots Snapshot store. Defaults to no-op.
  * @param assumeCurrentSnapshots Can snapshots from the snapshot store
  * be assumed to be current? I.e. are the snapshots available to all
  * running processes, or isolated to this process, and if the latter,
  * is this process exclusively snapshotting the available ids?
  * NOTE: This is a performance switch. Regardless of setting, it will
  * not affect correctness.
  */
class EntityRepository[ESID, EVT, S >: Null, ID, ET](
    entity: Entity[S, EVT] { type Id = ID; type Type = ET },
    exeCtx: ExecutionContext)(
    eventStore: EventStore[ESID, _ >: EVT],
    snapshots: SnapshotStore[ID, S] = SnapshotStore.empty[ID, S],
    assumeCurrentSnapshots: Boolean = false)(
    implicit idConv: ID => ESID)
  extends Repository[ID, ET] with MutableEntity {

  private[this] val repo =
    new EventStoreRepository(
        entity.channel, entity.newState, exeCtx, snapshots, assumeCurrentSnapshots)(eventStore)

  @inline implicit private def ec = exeCtx

  def exists(id: ID): Future[Option[Int]] = repo.exists(id)

  def load(id: ID): Future[(ET, Int)] = {
    repo.load(id).map {
      case ((state, _), revision) =>
        entity.initEntity(state, Nil) -> revision
    }(exeCtx)
  }

  protected def update[R](
      expectedRevision: Option[Int], id: ID,
      updateThunk: (ET, Int) => Future[R])(
      implicit
      metadata: Metadata): Future[(R, Int)] = {
    @volatile var returnValue = null.asInstanceOf[R]
    val futureRev = repo.update(id, expectedRevision) {
      case ((state, mergeEvents), revision) =>
        val instance = entity.initEntity(state, mergeEvents)
        updateThunk(instance, revision).map { ret =>
          returnValue = ret
          val state = entity.validatedState(instance)
          state.curr -> state.appliedEvents
        }
    }
    futureRev.map(returnValue -> _)
  }

  def insert(newId: => ID, instance: ET)(
      implicit
      metadata: Metadata): Future[ID] = {
    try {
      val state = entity.validatedState(instance)
      repo.insert(newId, state.curr -> state.appliedEvents)
    } catch {
      case NonFatal(e) => Future failed e
    }
  }

}
