package delta.write

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal

import delta._

/**
  * [[delta.write.Entity]]-based [[delta.write.Repository]] implementation.
  * @tparam SID Stream id type
  * @tparam EVT Repository event type
  * @tparam S Entity state representation. Must be an immutable type
  * @tparam ID Entity id type. Must be the same as, or translatable to, the event store id type
  * @tparam E The entity type
  * @param exeCtx ExecutionContext for basic Future transformations
  * @param entity Entity type class
  * @param eventStore The event store implementation
  * @param ticker Ticker implementation
  * @param snapshots Snapshot store. Defaults to no-op.
  * @param assumeCurrentRevisions Can snapshots from the snapshot store
  * be assumed to be current? I.e. are the snapshots available to all
  * running processes, or isolated to this process, and if the latter,
  * is this process exclusively snapshotting the available ids?
  * NOTE: This is a performance switch. Regardless of setting, it will
  * not affect correctness.
  */
class EntityRepository[SID, EVT, S >: Null, ID, E](
  entity: Entity[S, EVT] { type Id = ID; type Type = E })(
  eventStore: EventStore[SID, _ >: EVT],
  exeCtx: ExecutionContext,
  snapshots: SnapshotStore[ID, S] = SnapshotStore.empty[ID, S],
  assumeCurrentRevisions: Boolean = false)(
  implicit
  idConv: ID => SID)
extends Repository[ID, E]
with MutableEntity {

  import EventStoreRepository._

  private[this] val repo =
    new EventStoreRepository(
        entity.channel, entity.newStateRef, snapshots, assumeCurrentRevisions)(eventStore, exeCtx)

  protected def revision(loaded: Loaded) = loaded._2

  private implicit def ec = exeCtx

  def exists(id: ID): Future[Option[Revision]] = repo.exists(id)

  def load(id: ID): Future[(E, Revision)] = {
    repo.load(id).map {
      case Loaded(state, revision, _, _) =>
        entity.initEntity(id, state, Nil) -> revision
    }
  }

  protected def update(
      updateThunk: Loaded => Future[UpdateReturn],
      id: ID, expectedRevision: Option[Revision])
      : Future[Revision] = {

    repo.update(id, expectedRevision) {
      case Loaded(state, revision, _, concurrentUpdates) =>
        val instance = entity.initEntity(id, state, concurrentUpdates)
        updateThunk(instance -> revision)
          .map { metadata =>
            val state = entity.validatedState(instance)
            Save(state.get, state.appliedEvents) -> metadata
          }
    }

  }

  def insert(newId: => ID, instance: E)(implicit metadata: Metadata): Future[ID] = {
    try {
      val state = entity.validatedState(instance)
      repo.insert(newId, Save(state.get, state.appliedEvents))
    } catch {
      case NonFatal(e) => Future failed e
    }
  }

}
