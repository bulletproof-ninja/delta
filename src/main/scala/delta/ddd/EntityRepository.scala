package delta.ddd

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal

import delta.{ EventStore, Ticker }
import delta.SnapshotStore

/**
  * [[delta.ddd.Entity]]-based [[delta.ddd.Repository]] implementation.
  * @tparam ESID Event store id type
  * @tparam EVT Repository event type
  * @tparam CH Channel type
  * @tparam S Repository state type
  * @tparam ID Repository id type. Must be the same as, or translatable to, the event store id type
  * @tparam E The entity type
  * @param channel The channel
  * @param entity Entity type class
  * @param eventStore The event store implementation
  * @param snapshots Snapshot store. Defaults to no-op.
  * @param assumeCurrentSnapshots Can snapshots from the snapshot store
  * be assumed to be current? I.e. are the snapshots available to all
  * running processes, or isolated to this process, and if the latter,
  * is this process exclusively snapshotting the available ids?
  * NOTE: This is a performance switch. Regardless of setting, it will
  * not affect correctness.
  * @param exeCtx ExecutionContext for basic Future transformations
  * @param ticker Ticker implementation
  */
class EntityRepository[ESID, EVT, CH, S >: Null, ID <% ESID, E](
    channel: CH,
    entity: Entity { type Type = E; type Id = ID; type Mutator <: StateMutator { type State = S; type Event = EVT } })(
    eventStore: EventStore[ESID, _ >: EVT, CH],
    snapshots: SnapshotStore[ID, S] = SnapshotStore.empty[ID, S],
    assumeCurrentSnapshots: Boolean = false)(
    implicit exeCtx: ExecutionContext, ticker: Ticker)
  extends Repository[ID, E] with MutableEntity {

  private val repo = new EventStoreRepository(channel, entity.newMutator, snapshots, assumeCurrentSnapshots)(eventStore)

  def exists(id: ID): Future[Option[Int]] = repo.exists(id)

  def load(id: ID): Future[(E, Int)] = {
    repo.load(id).map {
      case ((state, _), revision) =>
        val mutator = entity.newMutator.init(state)
        entity.init(mutator, Nil) -> revision
    }(exeCtx)
  }

  protected def update[R](
      expectedRevision: Revision, id: ID,
      metadata: Map[String, String], updateThunk: (E, Int) => Future[R]): Future[(R, Int)] = {
    @volatile var returnValue = null.asInstanceOf[R]
    val futureRev = repo.update(id, expectedRevision, metadata) {
      case ((state, mergeEvents), revision) =>
        val instance = entity.init(entity.newMutator.init(state), mergeEvents)
        updateThunk(instance, revision).map { ret =>
          returnValue = ret
          val mutator = entity.getMutator(instance)
          mutator.state -> mutator.appliedEvents
        }
    }
    futureRev.map(returnValue -> _)
  }

  def insert(id: ID, instance: E, metadata: Map[String, String]): Future[Int] = {
    try {
      val mutator = entity.getMutator(instance)
      repo.insert(id, mutator.state -> mutator.appliedEvents, metadata)
    } catch {
      case NonFatal(e) => Future failed e
    }
  }

}
