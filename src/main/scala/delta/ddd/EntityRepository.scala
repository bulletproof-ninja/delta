package delta.ddd

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal

import scuff.ddd.Repository
import delta.{ EventStore, Ticker }
import delta.SnapshotStore

/**
  * [[delta.ddd.Entity]]-based [[scuff.ddd.Repository]] implementation.
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
    extends Repository[ID, E] {

  private val repo = new EventStoreRepository(channel, entity.newMutator, snapshots)(eventStore)

  def exists(id: ID): Future[Option[Int]] = repo.exists(id)

  def load(id: ID): Future[(E, Int)] = {
    repo.load(id).map {
      case ((state, _), revision) =>
        val mutator = entity.newMutator.init(state)
        entity.init(mutator, Nil) -> revision
    }(exeCtx)
  }

  def update(id: ID, expectedRevision: Option[Int], metadata: Map[String, String])(
    updateThunk: (E, Int) => Future[E]): Future[Int] = {
    repo.update(id, expectedRevision, metadata) {
      case ((state, mergeEvents), revision) =>
        val instance = entity.init(entity.newMutator.init(state), mergeEvents)
        updateThunk(instance, revision).map { ar =>
          val mutator = entity.getMutator(instance)
          mutator.state -> mutator.appliedEvents
        }(exeCtx)
    }
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
