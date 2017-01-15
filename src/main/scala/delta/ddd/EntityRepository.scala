package delta.ddd

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal

import scuff.ddd.Repository
import delta.{ EventStore, Ticker }

class EntityRepository[ESID, EVT, CH, S >: Null, ID <% ESID, E](
  channel: CH,
  entity: Entity { type Type = E; type Id = ID; type Mutator <: StateMutator { type State = S; type Event = EVT }})(
    eventStore: EventStore[ESID, _ >: EVT, CH],
    snapshots: SnapshotStore[ID, S] = SnapshotStore.Disabled[ID, S])(
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
