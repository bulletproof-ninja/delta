package ulysses.ddd

import scuff.ddd.Repository
import scala.concurrent.Future
import scuff.concurrent.Threads.PiggyBack
import scala.util.control.NonFatal
import scala.concurrent.ExecutionContext
import ulysses.EventStore

class EntityRepository[ESID, EVT, CH, S <: AnyRef, RID <% ESID, AR](
  clock: Clock,
  aggr: AggregateRoot { type Id = RID; type State = S; type Event = EVT; type Channel = CH; type Entity = AR })(
    eventStore: EventStore[ESID, _ >: EVT, CH],
    snapshots: SnapshotStore[RID, S] = SnapshotStore.Disabled[RID, S])(
      implicit ec: ExecutionContext)
    extends Repository[RID, AR] {

  private val repo = new StateRepository[ESID, EVT, CH, S, RID](
    clock, aggr.channel, eventStore, aggr.newMutator, snapshots)

  def exists(id: RID): Future[Option[Int]] = repo.exists(id)

  def load(id: RID): Future[(AR, Int)] = {
    repo.load(id).map {
      case ((state, _), revision) =>
        aggr.init(state, Vector.empty) -> revision
    }
  }

  def update(id: RID, expectedRevision: Option[Int], metadata: Map[String, String])(
    updateThunk: (AR, Int) => Future[AR]): Future[Int] = {
    repo.update(id, expectedRevision, metadata) {
      case ((state, mergeEvents), revision) =>
        val entity = aggr.init(state, mergeEvents)
        updateThunk(entity, revision).map { ar =>
          val mutator = aggr.getMutator(ar)
          aggr.checkInvariants(mutator.state)
          val events = mutator.appliedEvents
          mutator.state -> events
        }
    }
  }

  def insert(id: RID, ar: AR, metadata: Map[String, String]): Future[Int] = {
    try {
      val mutator = aggr.getMutator(ar)
      aggr.checkInvariants(mutator.state)
      repo.insert(id, mutator.state -> mutator.appliedEvents, metadata)
    } catch {
      case NonFatal(e) => Future failed e
    }
  }

}
