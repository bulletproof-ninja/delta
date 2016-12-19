package ulysses.ddd

import scala.concurrent.{ ExecutionContext, Future }
import scala.language.implicitConversions

import scuff.concurrent.{ StreamCallback, StreamPromise }
import scuff.ddd.{ DuplicateIdException, Repository, UnknownIdException }
import ulysses.EventStore
import scala.util.control.NonFatal
import ulysses.Clock
import ulysses.Transaction
import SnapshotStore._

/**
  * [[ulysses.EventStore]]-based [[scuff.ddd.Repository]] implementation.
  * @tparam ESID Event store id type
  * @tparam EVT Repository event type
  * @tparam CH Channel type
  * @tparam S Repository state type
  * @tparam RID Repository id type
  */
class EventStoreRepository[ESID, EVT, CH, S <: AnyRef, RID <% ESID](
  exeCtx: ExecutionContext,
  channel: CH,
  clock: Clock,
  es: EventStore[ESID, _ >: EVT, CH],
  newMutator: Option[S] => StateMutator[EVT, S],
  snapshots: SnapshotStore[RID, S] = SnapshotStore.Disabled)
    extends Repository[RID, (S, List[EVT])] {

  private[this] val eventStore = es.asInstanceOf[EventStore[ESID, EVT, CH]]

  private implicit def ec = exeCtx
  private type Events = List[EVT]
  private type RepoType = (S, Events)

  import snapshots.Snapshot

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
    replay: StreamCallback[TXN] => Unit): Future[Option[(Snapshot, List[EVT])]] = {
    //    case class Builder(applyEventsAfter: Int, stateOrNull: S, concurrentUpdates: List[EVT] = Nil, lastTxnOrNull: TXN = null)
    case class Builder(applyEventsAfter: Int, mutator: StateMutator[EVT, S], concurrentUpdates: List[EVT] = Nil, lastTxnOrNull: TXN = null)
    val initBuilder = Builder(snapshot.map(_.revision) getOrElse -1, newMutator(snapshot.map(_.state)))
    val futureBuilt = StreamPromise.fold(replay)(initBuilder) {
      case (b, txn) =>
        if (txn.revision > b.applyEventsAfter) {
          txn.events.foreach(b.mutator.mutate)
        }
        val newUpdates: List[EVT] =
          if (txn.revision > lastSeenRevision) b.concurrentUpdates ::: txn.events
          else b.concurrentUpdates
        b.copy(concurrentUpdates = newUpdates, lastTxnOrNull = txn)
    }
    futureBuilt.map {
      case b @ Builder(_, _, _, null) =>
        snapshot.map(_ -> Nil)
      case b @ Builder(_, mutator, concurrentUpdates, lastTxn) =>
        Some(Snapshot(mutator.state, lastTxn.revision, lastTxn.tick) -> concurrentUpdates)
    }
  }

  private def loadLatest(id: RID, snapshot: Future[Option[Snapshot]], assumeSnapshotCurrent: Boolean, expectedRevision: Option[Int]): Future[(Snapshot, List[EVT])] = {
    val lastSeenRevision = expectedRevision getOrElse Int.MaxValue
    val futureState: Future[Option[(Snapshot, List[EVT])]] = snapshot.flatMap { maybeSnapshot =>
      maybeSnapshot match {
        case Some(snapshot) =>
          if (lastSeenRevision == snapshot.revision && assumeSnapshotCurrent) {
            Future successful Some(snapshot -> Nil)
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

  final def load(id: RID): Future[((S, Nil.type), Int)] = loadLatest(id, snapshots.load(id), false, None).map {
    case (snapshot, _) => snapshot.state -> Nil -> snapshot.revision
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
        Future failed new IllegalStateException(s"Nothing to insert, $id has no events.")
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

  private def recordUpdate(id: RID, state: S, newRevision: Int, events: List[EVT], metadata: Map[String, String], tick: Long): Future[Int] = {
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
                  val mutator = newMutator(Some(snapshot.state))
                  conflict.events.foreach(mutator.mutate)
                  val latestSnapshot = Future successful Some(Snapshot(mutator.state, conflict.revision, conflict.tick))
                  onUpdateCollision(id, conflict.revision, conflict.channel)
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
