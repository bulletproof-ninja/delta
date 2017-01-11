package ulysses.ddd

import scuff.ddd.Repository
import scala.concurrent.Future
import scuff.concurrent.Threads.PiggyBack
import scala.util.control.NonFatal
import scala.concurrent.ExecutionContext
import ulysses.EventStore
import ulysses.Ticker

class EntityRepository[ESID, EVT, CH, S <: AnyRef, ARID <% ESID, AR](
  channel: CH,
  entity: Entity { type Type = AR; type Id = ARID; type State = S; type Event = EVT })(
    eventStore: EventStore[ESID, _ >: EVT, CH],
    snapshots: SnapshotStore[ARID, S] = SnapshotStore.Disabled[ARID, S])(
      implicit exeCtx: ExecutionContext, ticker: Ticker)
    extends Repository[ARID, AR] {

  private val repo = new EventStoreRepository(channel, entity.newMutator, snapshots)(eventStore)

  def exists(id: ARID): Future[Option[Int]] = repo.exists(id)

  def load(id: ARID): Future[(AR, Int)] = {
    repo.load(id).map {
      case ((state, _), revision) =>
        entity.init(state, Nil) -> revision
    }(exeCtx)
  }

  def update(id: ARID, expectedRevision: Option[Int], metadata: Map[String, String])(
    updateThunk: (AR, Int) => Future[AR]): Future[Int] = {
    repo.update(id, expectedRevision, metadata) {
      case ((state, mergeEvents), revision) =>
        val instance = entity.init(state, mergeEvents)
        updateThunk(instance, revision).map { ar =>
          val mutator = entity.done(ar)
          entity.checkInvariants(mutator.state)
          mutator.state -> mutator.appliedEvents
        }(exeCtx)
    }
  }

  def insert(id: ARID, instance: AR, metadata: Map[String, String]): Future[Int] = {
    try {
      val mutator = entity.done(instance)
      entity.checkInvariants(mutator.state)
      repo.insert(id, mutator.state -> mutator.appliedEvents, metadata)
    } catch {
      case NonFatal(e) => Future failed e
    }
  }

}
