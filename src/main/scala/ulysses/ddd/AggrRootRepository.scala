package ulysses.ddd

import scuff.ddd.Repository
import scala.collection.immutable.Seq
import scala.concurrent.Future
import scuff.concurrent.Threads.PiggyBack
import scala.util.control.NonFatal

class AggrRootRepository[AR, ID, EVT, ESID, CAT, S](
  aggr: AggrRoot[AR, ID, EVT, S],
  repo: EventStoreRepository[S, ID, EVT, ESID, CAT])
    extends Repository[AR, ID] {

  def exists(id: ID): Future[Option[Int]] = repo.exists(id)

  def load(id: ID): Future[(AR, Int)] = {
    repo.load(id).map {
      case ((state, concurrentEvents), revision) =>
        aggr(id, state, concurrentEvents) -> revision
    }(PiggyBack)
  }

  def update(id: ID, expectedRevision: Option[Int], metadata: Map[String, String])(updateThunk: (AR, Int) => Future[AR]): Future[Int] = {
    repo.update(id, expectedRevision, metadata) {
      case ((state, concurrentEvents), revision) =>
        val ar = aggr(id, state, concurrentEvents)
        updateThunk(ar, revision).map { ar =>
          aggr.checkInvariants(ar)
          aggr.currState(ar) -> aggr.newEvents(ar)
        }(PiggyBack)
    }
  }

  def insert(id: ID, ar: AR, metadata: Map[String, String]): Future[Int] = {
    try {
      aggr.checkInvariants(ar)
      repo.insert(id, aggr.currState(ar) -> aggr.newEvents(ar), metadata)
    } catch {
      case NonFatal(e) => Future failed e
    }
  }

}
