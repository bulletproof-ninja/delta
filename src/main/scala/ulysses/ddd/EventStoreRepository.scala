package ulysses.ddd

import ulysses._
import scuff.ddd._
import scala.concurrent._
import java.util.concurrent.TimeUnit
import scala.util.Failure
import scala.util.Success
import scala.annotation.implicitNotFound
import scala.util.control.NonFatal
import scuff.concurrent.{ StreamCallback, StreamResult }
import language.implicitConversions
import collection.immutable.{ Seq, Vector }
import collection.{ Seq => aSeq, Map => aMap }

trait StateEvolver[S, EVT] {
  def init(evt: EVT): S
  def next(state: S, evt: EVT): S
}

/**
  * [[ulysses.EventStore]]-based [[scuff.ddd.Repository]] implementation.
  * @tparam S State (data) type
  * @tparam ID State id type
  * @tparam EVT Event type
  * @tparam ESID Event store id type
  * @tparam CH State channel type
  */
class EventStoreRepository[S, ID, EVT, ESID, CH](
  eventStore: EventStore[ESID, EVT, CH],
  evolver: StateEvolver[S, EVT],
  clock: Clock,
  snapshots: SnapshotStore[S, ID] = SnapshotStore.NoSnapshots[S, ID])(
    implicit exeCtx: ExecutionContext, idConverter: ID => ESID, toChannel: S => CH)
    extends Repository[(S, Seq[EVT]), ID] {
  import snapshots.Snapshot
  import SnapshotStore._

  type StateAndEvents = (S, Seq[EVT])

  def exists(id: ID): Future[Option[Int]] = eventStore.currRevision(id)

  private type TXN = eventStore.TXN

  private[this] def loadRevision(id: ID, revision: Int): Future[(S, Int)] = {
    val futureState = StreamResult.fold(eventStore.replayStreamTo(id, revision))(None: Option[(S, Int)]) {
      case (stateRev, txn) =>
        txn.events.foldLeft(stateRev.map(_._1)) {
          case (None, evt) => Some(evolver.init(evt))
          case (Some(state), evt) => Some(evolver.next(state, evt))
        }.map(_ -> txn.revision)
    }
    futureState.map {
      case Some(stateRev) => stateRev
      case None => throw new UnknownIdException(id)
    }
  }

  private def buildCurrentSnapshot(
    snapshot: Option[Snapshot],
    lastSeenRevision: Int,
    replay: StreamCallback[TXN] => Unit): Future[Option[(Snapshot, Seq[EVT])]] = {
    case class Builder(applyEventsAfter: Int, stateOrNull: S, concurrentUpdates: Vector[EVT] = Vector.empty, lastTxnOrNull: TXN = null)
    val initBuilder = Builder(snapshot.map(_.revision) getOrElse -1, snapshot.map(_.state) getOrElse null.asInstanceOf[S])
    val futureBuilt = StreamResult.fold(replay)(initBuilder) {
      case (builder @ Builder(applyEventsAfter, stateOrNull, concurrentUpdates, _), txn) =>
        val state = stateOrNull match {
          case null =>
            val state = evolver.init(txn.events.head)
            txn.events.tail.foldLeft(state)(evolver.next)
          case state =>
            if (txn.revision > applyEventsAfter) {
              txn.events.foldLeft(state)(evolver.next)
            } else {
              state
            }
        }
        val newUpdates = if (txn.revision > lastSeenRevision) concurrentUpdates ++ txn.events else concurrentUpdates
        builder.copy(stateOrNull = state, concurrentUpdates = newUpdates, lastTxnOrNull = txn)
    }
    futureBuilt.map {
      case b @ Builder(_, _, _, null) =>
        snapshot.map(_ -> Nil)
      case b @ Builder(_, state, concurrentUpdates, lastTxn) =>
        Some(Snapshot(state, lastTxn.revision, lastTxn.tick) -> concurrentUpdates)
    }
  }

  private def loadLatest(id: ID, snapshot: Future[Option[Snapshot]], assumeSnapshotCurrent: Boolean, expectedRevision: Option[Int]): Future[(Snapshot, Seq[EVT])] = {
    val lastSeenRevision = expectedRevision getOrElse Int.MaxValue
    val futureState: Future[Option[(Snapshot, Seq[EVT])]] = snapshot.flatMap { maybeSnapshot =>
      maybeSnapshot match {
        case Some(snapshot) =>
          if (lastSeenRevision == snapshot.revision && assumeSnapshotCurrent) {
            Future successful Some(snapshot, Nil)
          } else {
            val replayFromRev = (snapshot.revision min lastSeenRevision) + 1
            buildCurrentSnapshot(maybeSnapshot, lastSeenRevision, eventStore.replayStreamFrom(id, replayFromRev))
          }
        case None =>
          buildCurrentSnapshot(maybeSnapshot, lastSeenRevision, eventStore.replayStream(id))
      }
    }
    futureState.map(opt => opt.getOrElse(throw new UnknownIdException(id)))
  }

  final def load(id: ID): Future[(StateAndEvents, Int)] = loadLatest(id, snapshots.load(id), false, None).map {
    case (snapshot, nil) => snapshot.state -> nil -> snapshot.revision
  }

  //  final def load(id: ID): Future[(StateAndEvents, Int)] = load(id, None).map {
  //    case (ar, rev) => ar -> Nil -> rev
  //  }

  //  def load(id: ID, revision: Option[Int]): Future[(AR, Int)] = revision match {
  //    case Some(revision) => loadRevision(id, revision)
  //    case None => loadLatest(id, snapshots.load(id), false, None).map(t => t._1._1 -> t._2.revision)
  //  }

  def insert(id: ID, stateAndEvts: StateAndEvents, metadata: Map[String, String]): Future[Int] = {
    val (state, events) = stateAndEvts
    if (events.isEmpty) {
      val channel: CH = state
      Future failed new IllegalStateException(s"Cannot insert, since $channel $id has not produced events.")
    } else {
      val now = clock.nextTick()
      eventStore.record(state, id, 0, now, events, metadata).map(_.revision).recoverWith {
        case eventStore.DuplicateRevisionException(ct) =>
          if (ct.events == events && ct.metadata == metadata) {
            // Idempotent success
            Future successful ct.revision
          } else {
            Future failed new DuplicateIdException(id)
          }
      }
    }
  }

  private def recordUpdate(id: ID, state: S, newRevision: Int, events: Seq[EVT], metadata: Map[String, String], tick: Long): Future[Int] = {
    eventStore.record(state, id, newRevision, tick, events, metadata).map { txn =>
      snapshots.save(id, Snapshot(state, newRevision, tick))
      txn.revision
    }
  }

  /**
    * Notification on every concurrent update collision.
    * This happens when two aggregates are attempted to
    * be updated concurrently. Should be exceedingly rare,
    * unless there is unusually high contention on a
    * specific aggregate.
    * Can be used for monitoring and reporting.
    */
  protected def onConcurrentUpdateCollision(id: ID, revision: Int, channel: CH): Unit = ()

  private def loadAndUpdate(
    id: ID, expectedRevision: Option[Int], metadata: Map[String, String],
    maybeSnapshot: Future[Option[Snapshot]], assumeSnapshotCurrent: Boolean,
    handler: (StateAndEvents, Int) => Future[StateAndEvents]): Future[Int] = {

    loadLatest(id, maybeSnapshot, assumeSnapshotCurrent, expectedRevision).flatMap {
      case (snapshot, concurrentUpdates) =>
        handler(snapshot.state -> concurrentUpdates, snapshot.revision).flatMap {
          case (state, newEvents) =>
            if (newEvents.isEmpty || concurrentUpdates == newEvents) {
              Future successful snapshot.revision
            } else {
              val now = clock.nextTick(snapshot.tick)
              recordUpdate(id, state, snapshot.revision + 1, newEvents, metadata, now).recoverWith {
                case eventStore.DuplicateRevisionException(conflict) =>
                  val newState = conflict.events.foldLeft(snapshot.state)(evolver.next)
                  val latestSnapshot = Future successful Some(Snapshot(newState, conflict.revision, conflict.tick))
                  onConcurrentUpdateCollision(id, conflict.revision, state)
                  loadAndUpdate(id, expectedRevision, metadata, latestSnapshot, true, handler)
              }
            }
        }
    }
  }

  def update(id: ID, expectedRevision: Option[Int], metadata: Map[String, String] = Map.empty)(updateThunk: (StateAndEvents, Int) => Future[StateAndEvents]): Future[Int] = {
    loadAndUpdate(id, expectedRevision, metadata, snapshots.load(id), snapshots.assumeSnapshotCurrent, updateThunk)
  }

}
