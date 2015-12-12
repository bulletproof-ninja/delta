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

trait StateMachine[S, EVT] {
  def init(evt: EVT): S
  def transition(state: S, evt: EVT): S
}

trait Timestamps {
  /**
   * Produce a new clock. The new clock must always
   * be greater than the passed clock, to ensure global
   * ordering of causal events.
   */
  def nextTimestamp(greaterThan: Long = Long.MinValue): Long
}

/**
 * Snapshot storage implementation.
 */
trait SnapshotStore[S, ID] {
  type Snapshot = SnapshotStore.Snapshot[S]

  /** Load snapshot, if exists. */
  def load(id: ID): Future[Option[Snapshot]]
  /**
   *  Save snapshot.
   *  This method should not throw an exception,
   *  but handle/report it internally.
   */
  def save(id: ID, snapshot: Snapshot): Unit

  /**
   * Should a snapshot be considered current? Defaults to `false`.
   * Basically this should only be overridden if all snapshots
   * are saved (in centralized location if distributed environment).
   * Assuming snapshots are current will save a query to event store,
   * if actually true. If not true (due to race conditions or snapshots
   * not always stored), the event store will still be queried after
   * detecting non-currency. In other words, having this be `true`
   * will still work, but be less efficient if not actually
   * true most of the time.
   */
  def assumeSnapshotCurrent = false

}
object SnapshotStore {
  /**
   * @param Snapshot state
   * @param Snapshot revision
   * @param Transaction clock
   */
  case class Snapshot[S](state: S, revision: Int, clock: Long)

  private[this] val Empty = new SnapshotStore[AnyRef, Any] {
    private[this] val NoSnapshot = Future.successful(None)
    def load(id: Any): Future[Option[Snapshot]] = NoSnapshot
    def save(id: Any, snapshot: Snapshot) {}
  }

  def NoSnapshots[S, ID]: SnapshotStore[S, ID] = Empty.asInstanceOf[SnapshotStore[S, ID]]
}

/**
 * [[ulysses.EventStore]]-based [[scuff.ddd.Repository]] implementation.
 * @tparam S State (data) type
 * @tparam ID State id type
 * @tparam EVT Event type
 * @tparam ESID Event store id type
 * @tparam CAT State category type
 */
class EventStoreRepository[S, ID, EVT, ESID, CAT](
  eventStore: EventStore[ESID, EVT, CAT],
  sm: StateMachine[S, EVT],
  clock: Timestamps,
  snapshots: SnapshotStore[S, ID] = SnapshotStore.NoSnapshots[S, ID])(implicit exeCtx: ExecutionContext, idConverter: ID => ESID, categorizer: S => CAT)
    extends Repository[(S, Seq[EVT]), ID] {
  import snapshots.Snapshot
  import SnapshotStore._

  type SwithEVTs = (S, Seq[EVT])

  def exists(id: ID): Future[Option[Int]] = eventStore.currRevision(id)

  private type TXN = eventStore.Transaction

  private[this] def loadRevision(id: ID, revision: Int): Future[(S, Int)] = {
    val futureState = StreamResult.fold(eventStore.replayStreamTo(id, revision))(None: Option[(S, Int)]) {
      case (stateRev, txn) =>
        txn.events.foldLeft(stateRev.map(_._1)) {
          case (None, evt) => Some(sm.init(evt))
          case (Some(state), evt) => Some(sm.transition(state, evt))
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
            val state = sm.init(txn.events.head)
            txn.events.tail.foldLeft(state)(sm.transition)
          case state =>
            if (txn.revision > applyEventsAfter) {
              txn.events.foldLeft(state)(sm.transition)
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
        Some(Snapshot(state, lastTxn.revision, lastTxn.clock) -> concurrentUpdates)
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

  final def load(id: ID): Future[(SwithEVTs, Int)] = loadLatest(id, snapshots.load(id), false, None).map {
    case (snapshot, nil) => snapshot.state -> nil -> snapshot.revision
  }

  //  final def load(id: ID): Future[(SwithEVTs, Int)] = load(id, None).map {
  //    case (ar, rev) => ar -> Nil -> rev
  //  }

  //  def load(id: ID, revision: Option[Int]): Future[(AR, Int)] = revision match {
  //    case Some(revision) => loadRevision(id, revision)
  //    case None => loadLatest(id, snapshots.load(id), false, None).map(t => t._1._1 -> t._2.revision)
  //  }

  def insert(id: ID, stateWithEvts: SwithEVTs, metadata: Map[String, String]): Future[Int] = {
    val (state, events) = stateWithEvts
    if (events.isEmpty) {
      Future.failed(new IllegalStateException(s"Cannot insert, since ${state: CAT} $id has produced no events."))
    } else {
      val clock = clock.nextTimestamp()
      eventStore.record(clock, state, id, 0, events, metadata).map(_.revision).recoverWith {
        case dre: eventStore.DuplicateRevisionException =>
          import dre.{ conflictingTransaction => ct }
          if (ct.events == events && ct.metadata == metadata) {
            // Idempotent success
            exists(id).map(_.get)
          } else {
            Future failed new DuplicateIdException(id)
          }
      }
    }
  }

  private[this] def recordUpdate(id: ID, ar: S, newRevision: Int, events: Seq[EVT], metadata: Map[String, String], clock: Long): Future[Int] = {
    eventStore.record(clock, ar, id, newRevision, events, metadata).map { txn =>
      snapshots.save(id, Snapshot(ar, newRevision, clock))
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
  protected def onConcurrentUpdateCollision(id: ID, revision: Int, category: CAT) {}

  private def loadAndUpdate(
    id: ID, expectedRevision: Option[Int], metadata: Map[String, String],
    maybeSnapshot: Future[Option[Snapshot]], assumeSnapshotCurrent: Boolean,
    handler: (SwithEVTs, Int) => Future[SwithEVTs]): Future[Int] = {

    loadLatest(id, maybeSnapshot, assumeSnapshotCurrent, expectedRevision).flatMap {
      case (snapshot, concurrentUpdates) =>
        handler(snapshot.state -> concurrentUpdates, snapshot.revision).flatMap {
          case (ar, newEvents) =>
            if (newEvents.isEmpty || concurrentUpdates == newEvents) {
              Future successful snapshot.revision
            } else {
              val clock = clock.nextTimestamp(snapshot.clock)
              recordUpdate(id, ar, snapshot.revision + 1, newEvents, metadata, clock).recoverWith {
                case dre: eventStore.DuplicateRevisionException =>
                  val newState = dre.conflictingTransaction.events.foldLeft(snapshot.state)(sm.transition)
                  val latestSnapshot = Future successful Some(Snapshot(newState, dre.conflictingTransaction.revision, dre.conflictingTransaction.clock))
                  onConcurrentUpdateCollision(id, dre.conflictingTransaction.revision, ar)
                  loadAndUpdate(id, expectedRevision, metadata, latestSnapshot, true, handler)
              }
            }
        }
    }
  }

  def update(id: ID, expectedRevision: Option[Int], metadata: Map[String, String] = Map.empty)(updateThunk: (SwithEVTs, Int) => Future[SwithEVTs]): Future[Int] = {
    loadAndUpdate(id, expectedRevision, metadata, snapshots.load(id), snapshots.assumeSnapshotCurrent, updateThunk)
  }

}
