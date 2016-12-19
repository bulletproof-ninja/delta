package ulysses.ddd

import scuff.ddd.Repository
import scala.concurrent.Future
import scuff.concurrent.Threads.PiggyBack
import scala.util.control.NonFatal
import scala.concurrent.ExecutionContext
import ulysses.EventStore
import ulysses.Clock

class EntityRepository[ESID, EVT, CH, S <: AnyRef, ARID <% ESID, AR](
  exeCtx: ExecutionContext,
  clock: Clock,
  aggr: AggregateRoot { type Id = ARID; type State = S; type Event = EVT; type Channel = CH; type Entity = AR })(
    eventStore: EventStore[ESID, _ >: EVT, CH],
    snapshots: SnapshotStore[ARID, S] = SnapshotStore.Disabled[ARID, S])
    extends Repository[ARID, AR] {

  private val repo = new EventStoreRepository[ESID, EVT, CH, S, ARID](
      exeCtx, aggr.channel, clock, eventStore, aggr.newMutator, snapshots)

  def exists(id: ARID): Future[Option[Int]] = repo.exists(id)

  def load(id: ARID): Future[(AR, Int)] = {
    repo.load(id).map {
      case ((state, _), revision) =>
        aggr.init(state, Nil) -> revision
    }(exeCtx)
  }

  def update(id: ARID, expectedRevision: Option[Int], metadata: Map[String, String])(
    updateThunk: (AR, Int) => Future[AR]): Future[Int] = {
    repo.update(id, expectedRevision, metadata) {
      case ((state, mergeEvents), revision) =>
        val entity = aggr.init(state, mergeEvents)
        updateThunk(entity, revision).map { ar =>
          val mutator = aggr.done(ar)
          aggr.checkInvariants(mutator.state)
          mutator.state -> mutator.appliedEvents
        }(exeCtx)
    }
  }

  def insert(id: ARID, ar: AR, metadata: Map[String, String]): Future[Int] = {
    try {
      val mutator = aggr.done(ar)
      aggr.checkInvariants(mutator.state)
      repo.insert(id, mutator.state -> mutator.appliedEvents, metadata)
    } catch {
      case NonFatal(e) => Future failed e
    }
  }

}
