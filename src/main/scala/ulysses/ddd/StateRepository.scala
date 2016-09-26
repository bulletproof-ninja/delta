package ulysses.ddd

import scala.concurrent.{ ExecutionContext, Future }
import scala.language.implicitConversions

import scuff.concurrent.{ StreamCallback, StreamPromise }
import scuff.ddd.{ DuplicateIdException, Repository, UnknownIdException }
import ulysses.EventStore
import scala.util.control.NonFatal

/**
  * [[ulysses.EventStore]]-based [[scuff.ddd.Repository]] implementation.
  * @tparam ESID Event store id type
  * @tparam EVT Repository event type
  * @tparam CH Channel type
  * @tparam S Repository state type
  * @tparam RID Repository id type
  */
class StateRepository[ESID, EVT, CH, S <: AnyRef, RID <% ESID](
  clock: Clock,
  channel: CH,
  eventStore: EventStore[ESID, _ >: EVT, _ >: CH],
  newMutator: Option[S] => StateMutator[EVT, S],
  snapshots: SnapshotStore[RID, S] = SnapshotStore.Disabled)(
    implicit exeCtx: ExecutionContext)
    extends Repository[RID, (S, Vector[EVT])] {

  private type Events = Vector[EVT]
  private type RepoType = (S, Events)
  import SnapshotStore._
  import snapshots.Snapshot

  private[this] val mutator = new StatelessMutator(newMutator)

  def exists(id: RID): Future[Option[Int]] = eventStore.currRevision(id)

  private type TXN = eventStore.TXN

  //  private[this] def loadRevision(id: RID, revision: Int): Future[(S, Int)] = {
  //    val futureState = StreamPromise.fold(eventStore.replayStreamTo(id, revision))(None: Option[(S, Int)]) {
  //      case (stateRev, txn) =>
  //        txn.events.foldLeft(stateRev.map(_._1)) {
  //          case (Some(state), evt) => Some(mutator.next(state, evt))
  //          case (None, evt) => Some(mutator.init(evt))
  //        }.map(_ -> txn.revision)
  //    }
  //    futureState.map {
  //      case Some(stateRev) => stateRev
  //      case None => throw new UnknownIdException(id)
  //    }
  //  }

  private def buildCurrentSnapshot(
    snapshot: Option[Snapshot],
    lastSeenRevision: Int,
    replay: StreamCallback[TXN] => Unit): Future[Option[(Snapshot, Vector[_ >: EVT])]] = {
    case class Builder(applyEventsAfter: Int, stateOrNull: S, concurrentUpdates: Vector[_ >: EVT] = Vector.empty, lastTxnOrNull: TXN = null)
    val initBuilder = Builder(snapshot.map(_.revision) getOrElse -1, snapshot.map(_.state) getOrElse null.asInstanceOf[S])
    val futureBuilt = StreamPromise.fold(replay)(initBuilder) {
      case (builder @ Builder(applyEventsAfter, stateOrNull, concurrentUpdates, _), txn) =>
        val state = stateOrNull match {
          case null =>
            val state = mutator.init(txn.events.head.asInstanceOf[EVT])
            txn.events.asInstanceOf[Events].tail.foldLeft(state)(mutator.next)
          case state =>
            if (txn.revision > applyEventsAfter) {
              txn.events.asInstanceOf[Events].foldLeft(state)(mutator.next)
            } else {
              state
            }
        }
        val newUpdates = if (txn.revision > lastSeenRevision) concurrentUpdates ++ txn.events else concurrentUpdates
        builder.copy(stateOrNull = state, concurrentUpdates = newUpdates, lastTxnOrNull = txn)
    }
    futureBuilt.map {
      case b @ Builder(_, _, _, null) =>
        snapshot.map(_ -> Vector.empty)
      case b @ Builder(_, state, concurrentUpdates, lastTxn) =>
        Some(Snapshot(state, lastTxn.revision, lastTxn.tick) -> concurrentUpdates)
    }
  }

  private def loadLatest(id: RID, snapshot: Future[Option[Snapshot]], assumeSnapshotCurrent: Boolean, expectedRevision: Option[Int]): Future[(Snapshot, Vector[_ >: EVT])] = {
    val lastSeenRevision = expectedRevision getOrElse Int.MaxValue
    val futureState: Future[Option[(Snapshot, Vector[_ >: EVT])]] = snapshot.flatMap { maybeSnapshot =>
      maybeSnapshot match {
        case Some(snapshot) =>
          if (lastSeenRevision == snapshot.revision && assumeSnapshotCurrent) {
            Future successful Some(snapshot -> Vector.empty)
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

  final def load(id: RID): Future[(RepoType, Int)] = loadLatest(id, snapshots.load(id), false, None).map {
    case (snapshot, _) => snapshot.state -> Vector.empty -> snapshot.revision
  }

  //  final def load(id: RID): Future[(RepoType, Int)] = load(id, None).map {
  //    case (ar, rev) => ar -> Nil -> rev
  //  }

  //  def load(id: RID, revision: Option[Int]): Future[(S, Int)] = revision match {
  //    case Some(revision) => loadRevision(id, revision)
  //    case None => loadLatest(id, snapshots.load(id), false, None).map(t => t._1._1 -> t._2.revision)
  //  }

  def insert(id: RID, stEvt: RepoType, metadata: Map[String, String]): Future[Int] = stEvt match {
    case (state, events) =>
      if (events.isEmpty) {
        Future failed new IllegalStateException(s"Cannot insert, since $channel $id has no events.")
      } else {
        val tick = clock.nextTick()
        val committedRevision = eventStore.commit(channel, id, 0, tick, events, metadata).map(_.revision).recoverWith {
          case eventStore.DuplicateRevisionException(ct) =>
            if (ct.events == events && ct.metadata == metadata) {
              // Idempotent insert
              Future successful ct.revision
            } else {
              Future failed new DuplicateIdException(id)
            }
        }
        committedRevision.foreach(rev => snapshots.save(id, new Snapshot(state, rev, tick)))
        committedRevision
      }
  }

  private def recordUpdate(id: RID, state: S, newRevision: Int, events: Seq[EVT], metadata: Map[String, String], tick: Long): Future[Int] = {
    val committedRevision = eventStore.commit(channel, id, newRevision, tick, events, metadata).map(_.revision)
    committedRevision.foreach(rev => snapshots.save(id, new Snapshot(state, rev, tick)))
    committedRevision
  }

  /**
    * Notification on every concurrent update collision.
    * This happens when two aggregates are attempted to
    * be updated concurrently. Should be exceedingly rare,
    * unless there is unusually high contention on a
    * specific aggregate.
    * Can be used for monitoring and reporting.
    */
  protected def onUpdateCollision(id: RID, revision: Int, ar: CH): Unit = ()

  private def loadAndUpdate(
    id: RID, expectedRevision: Option[Int], metadata: Map[String, String],
    maybeSnapshot: Future[Option[Snapshot]], assumeSnapshotCurrent: Boolean,
    updateThunk: (RepoType, Int) => Future[RepoType]): Future[Int] = {

    loadLatest(id, maybeSnapshot, assumeSnapshotCurrent, expectedRevision).flatMap {
      case (snapshot, mergeEvents) =>
        updateThunk((snapshot.state, mergeEvents.asInstanceOf[Events]), snapshot.revision).flatMap {
          case (state, newEvents) =>
            if (newEvents.isEmpty || mergeEvents == newEvents) {
              Future successful snapshot.revision
            } else {
              val now = clock.nextTick(snapshot.tick)
              recordUpdate(id, state, snapshot.revision + 1, newEvents, metadata, now).recoverWith {
                case eventStore.DuplicateRevisionException(conflict) =>
                  val newState = conflict.events.asInstanceOf[Events].foldLeft(snapshot.state)(mutator.next)
                  val latestSnapshot = Future successful Some(Snapshot(newState, conflict.revision, conflict.tick))
                  onUpdateCollision(id, conflict.revision, channel)
                  loadAndUpdate(id, expectedRevision, metadata, latestSnapshot, true, updateThunk)
              }
            }
        }
    }
  }

  def update(
    id: RID, expectedRevision: Option[Int], metadata: Map[String, String])(
      updateThunk: (RepoType, Int) => Future[RepoType]): Future[Int] = try {
    loadAndUpdate(id, expectedRevision, metadata, snapshots.load(id), snapshots.assumeSnapshotCurrent, updateThunk)
  } catch {
    case NonFatal(th) => Future failed th
  }

}
