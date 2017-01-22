package delta.ddd

import scala.concurrent.{ ExecutionContext, Future }

import scuff.concurrent.{ StreamCallback, StreamPromise }
import scuff.ddd.{ DuplicateIdException, Repository, UnknownIdException }
import delta.EventStore
import scala.util.control.NonFatal
import delta.Ticker
import delta.SnapshotStore

/**
  * [[delta.EventStore]]-based [[scuff.ddd.Repository]] implementation.
  * @tparam ESID Event store id type
  * @tparam EVT Repository event type
  * @tparam CH Channel type
  * @tparam S Repository state type
  * @tparam RID Repository id type
  * @param channel The channel
  * @param newMutator StateMutator constructor
  * @param snapshots Snapshot store. Defaults to no-op.
  * @param assumeCurrentSnapshots Can snapshots from the snapshot store
  * be assumed to be current? I.e. are the snapshots available to all
  * running processes, or isolated to this process, and if the latter,
  * is this process exclusively snapshotting the available ids?
  * NOTE: This is a performance switch. Regardless of setting, it will
  * not affect correctness.
  * @param es The event store implementation
  * @param exeCtx ExecutionContext for basic Future transformations
  * @param ticker Ticker implementation
  */
class EventStoreRepository[ESID, EVT, CH, S >: Null, RID <% ESID](
  channel: CH,
  newMutator: => StateMutator { type Event = EVT; type State = S },
  snapshots: SnapshotStore[RID, S] = SnapshotStore.empty[RID, S],
  assumeCurrentSnapshots: Boolean = false)(
    es: EventStore[ESID, _ >: EVT, CH])(
      implicit exeCtx: ExecutionContext, ticker: Ticker)
    extends Repository[RID, (S, List[EVT])] {

  private type Snapshot = delta.Snapshot[S]
  private type Mutator = StateMutator { type Event = EVT; type State = S }

  private[this] val eventStore = es.asInstanceOf[EventStore[ESID, EVT, CH]]

  private type Events = List[EVT]
  private type RepoType = (S, Events)

  def exists(id: RID): Future[Option[Int]] = eventStore.currRevision(id)

  private type TXN = eventStore.TXN

  private def buildCurrentSnapshot(
    snapshot: Option[Snapshot],
    lastSeenRevision: Int,
    replay: StreamCallback[TXN] => Unit): Future[Option[(Snapshot, List[EVT])]] = {
    //    case class Builder(applyEventsAfter: Int, stateOrNull: S, concurrentUpdates: List[EVT] = Nil, lastTxnOrNull: TXN = null)
    case class Builder(applyEventsAfter: Int, mutator: Mutator, concurrentUpdates: List[EVT] = Nil, lastTxnOrNull: TXN = null)
    val initBuilder = Builder(snapshot.map(_.revision) getOrElse -1, newMutator.init(snapshot.map(_.content)))
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
        Some(new Snapshot(mutator.state, lastTxn.revision, lastTxn.tick) -> concurrentUpdates)
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

  final def load(id: RID): Future[((S, Nil.type), Int)] = loadLatest(id, snapshots.get(id), false, None).map {
    case (snapshot, _) => snapshot.content -> Nil -> snapshot.revision
  }

  def insert(id: RID, stEvt: RepoType, metadata: Map[String, String]): Future[Int] = stEvt match {
    case (state, events) =>
      if (events.isEmpty) {
        Future failed new IllegalStateException(s"Nothing to insert, $id has no events.")
      } else {
        val tick = ticker.nextTick()
        val committedRevision = eventStore.commit(channel, id, 0, tick, events, metadata).map(_.revision) recover {
          case eventStore.DuplicateRevisionException(ct) =>
            if (ct.events == events) { // Idempotent insert
              ct.revision
            } else {
              throw new DuplicateIdException(id)
            }
        }
        committedRevision.foreach(rev => snapshots.set(id, new Snapshot(state, rev, tick)))
        committedRevision
      }
  }

  private def recordUpdate(id: RID, state: S, newRevision: Int, events: List[EVT], metadata: Map[String, String], tick: Long): Future[Int] = {
    val committedRevision = eventStore.commit(channel, id, newRevision, tick, events, metadata).map(_.revision)
    committedRevision.foreach(rev => snapshots.set(id, new Snapshot(state, rev, tick)))
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
        updateThunk((snapshot.content, mergeEvents.asInstanceOf[Events]), snapshot.revision).flatMap {
          case (state, newEvents) =>
            if (newEvents.isEmpty || mergeEvents == newEvents) {
              Future successful snapshot.revision
            } else {
              val now = ticker.nextTick(snapshot.tick)
              recordUpdate(id, state, snapshot.revision + 1, newEvents, metadata, now).recoverWith {
                case eventStore.DuplicateRevisionException(conflict) =>
                  val mutator = newMutator.init(snapshot.content)
                  conflict.events.foreach(mutator.mutate)
                  val latestSnapshot = Future successful Some(new Snapshot(mutator.state, conflict.revision, conflict.tick))
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
    loadAndUpdate(id, expectedRevision, metadata, snapshots.get(id), assumeCurrentSnapshots, updateThunk)
  } catch {
    case NonFatal(th) => Future failed th
  }

}
