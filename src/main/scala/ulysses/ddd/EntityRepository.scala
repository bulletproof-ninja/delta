package ulysses.ddd

import scuff.ddd.Repository
import scala.collection.immutable.Seq
import scala.concurrent.Future
import scuff.concurrent.Threads.PiggyBack
import scala.util.control.NonFatal
import scala.concurrent.ExecutionContext
import ulysses.EventStore

class AggrRootRepository[ESID, EVT, CH, S <: AnyRef <% CH, ID <% ESID, AR <: AnyRef <% CH](
  eventStore: EventStore[ESID, EVT, CH],
  entity: Entity[AR, ID, EVT, S],
  clock: Clock,
  snapshots: SnapshotStore[S, ID] = SnapshotStore.NoSnapshots[S, ID])(
    implicit ec: ExecutionContext)
    extends Repository[AR, ID] {

  private val repo = new EventStoreRepository(eventStore, entity.newMutator, clock, snapshots)

  def exists(id: ID): Future[Option[Int]] = repo.exists(id)

  def load(id: ID): Future[(AR, Int)] = {
    repo.load(id).map {
      case ((state, _), revision) =>
        entity.init(id, state, Nil) -> revision
    }
  }

  def update(id: ID, expectedRevision: Option[Int], metadata: Map[String, String])(
    updateThunk: (AR, Int) => Future[AR]): Future[Int] = {
    repo.update(id, expectedRevision, metadata) {
      case ((state, mergeEvents), revision) =>
        val ar = entity.init(id, state, mergeEvents)
        updateThunk(ar, revision).map { ar =>
          val mutator = entity.getMutator(ar)
          entity.checkInvariants(mutator.state)
          mutator.state -> mutator.appliedEvents
        }
    }
  }

  def insert(id: ID, ar: AR, metadata: Map[String, String]): Future[Int] = {
    try {
      val mutator = entity.getMutator(ar)
      entity.checkInvariants(mutator.state)
      repo.insert(id, mutator.state -> mutator.appliedEvents, metadata)
    } catch {
      case NonFatal(e) => Future failed e
    }
  }

}
