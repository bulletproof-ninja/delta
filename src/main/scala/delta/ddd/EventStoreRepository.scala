package delta.ddd

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal

import delta.{ EventStore, SnapshotStore }
import delta.Transaction.Channel
import scuff.StreamConsumer
import scuff.concurrent.StreamPromise

/**
 * [[delta.EventStore]]-based [[delta.ddd.Repository]] implementation.
 * @tparam ESID Event store id type
 * @tparam EVT Repository event type
 * @tparam S Repository state type
 * @tparam RID Repository id type
 * @param channel The channel
 * @param newState Wrap state
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
class EventStoreRepository[ESID, EVT, S >: Null, RID](
    channel: Channel,
    newState: S => State[S, EVT],
    exeCtx: ExecutionContext,
    snapshots: SnapshotStore[RID, S] = SnapshotStore.empty[RID, S],
    assumeCurrentSnapshots: Boolean = false)(
    es: EventStore[ESID, _ >: EVT])(
    implicit
    idConv: RID => ESID)
  extends Repository[RID, (S, List[EVT])] with ImmutableEntity[(S, List[EVT])] {

  @inline implicit private def ec = exeCtx

  private type Snapshot = delta.Snapshot[S]

  private[this] val eventStore = es.asInstanceOf[EventStore[ESID, EVT]]
  private[this] val ticker = eventStore.ticker

  private type Events = List[EVT]
  private type RepoType = (S, Events)

  def exists(id: RID): Future[Option[Int]] = eventStore.currRevision(id)

  private type TXN = eventStore.TXN

  private def buildCurrentSnapshot(
      snapshot: Option[Snapshot],
      expectedRevision: Option[Int],
      replayer: StreamConsumer[TXN, Any] => Unit): Future[Option[(Snapshot, List[EVT])]] = {
    case class Builder(applyEventsAfter: Int, state: State[S, EVT], concurrentUpdates: List[EVT] = Nil, lastTxnOrNull: TXN = null)
    val initBuilder = new Builder(snapshot.map(_.revision) getOrElse -1, newState(snapshot.map(_.content).orNull))
    val lastSeenRevision = expectedRevision getOrElse Int.MaxValue
    val futureBuilt = StreamPromise.fold(initBuilder, replayer) {
      case (b, txn) =>
        if (txn.revision > b.applyEventsAfter) {
          txn.events.foreach(b.state.mutate)
        }
        val newUpdates: List[EVT] =
          if (txn.revision > lastSeenRevision) b.concurrentUpdates ::: txn.events
          else b.concurrentUpdates
        b.copy(concurrentUpdates = newUpdates, lastTxnOrNull = txn)
    }
    futureBuilt.map {
      case Builder(_, _, _, null) =>
        snapshot.map(_ -> Nil)
      case Builder(_, mutator, concurrentUpdates, lastTxn) =>
        Some(new Snapshot(mutator.curr, lastTxn.revision, lastTxn.tick) -> concurrentUpdates)
    }
  }

  private def loadLatest(id: RID, snapshot: Future[Option[Snapshot]], assumeSnapshotCurrent: Boolean, expectedRevision: Option[Int]): Future[(Snapshot, List[EVT])] = {
    // Failed snapshot read should not prevent loading. Report and continue...
    val recoveredSnapshot = snapshot recover {
      case NonFatal(th) =>
        exeCtx reportFailure th
        None
    }
    val futureState: Future[Option[(Snapshot, List[EVT])]] = recoveredSnapshot.flatMap { maybeSnapshot =>
      maybeSnapshot match {
        case Some(snapshot) =>
          if (assumeSnapshotCurrent && expectedRevision.forall(_ <= snapshot.revision)) {
            Future successful Some(snapshot -> Nil)
          } else {
            val minRev = expectedRevision.map(math.min(_, snapshot.revision)) getOrElse snapshot.revision
            buildCurrentSnapshot(maybeSnapshot, expectedRevision, eventStore.replayStreamFrom(id, minRev + 1))
          }
        case None =>
          buildCurrentSnapshot(maybeSnapshot, expectedRevision, eventStore.replayStream(id))
      }
    }
    futureState.map(opt => opt.getOrElse(throw new UnknownIdException(id)))
  }

  final def load(id: RID): Future[((S, Nil.type), Int)] = loadLatest(id, snapshots.read(id), false, None).map {
    case (snapshot, _) => snapshot.content -> Nil -> snapshot.revision
  }

  def insert(newId: => RID, stEvt: RepoType)(
      implicit
      metadata: Metadata): Future[RID] =
    insertImpl(newId, newId, ticker.nextTick(), stEvt, metadata.toMap)

  private def insertImpl(
      id: RID, generateId: => RID, tick: Long,
      stEvt: RepoType, metadata: Map[String, String],
      retries: Int = 256): Future[RID] =
    stEvt match {
      case (state, events) =>
        if (events.isEmpty) {
          Future failed new IllegalStateException(s"Nothing to insert, $id has no events.")
        } else {
          val committedId: Future[RID] =
            eventStore.commit(channel, id, 0, tick, events, metadata).map(_ => id) recoverWith {
              case dupe: eventStore.DuplicateRevisionException =>
                if (dupe.conflict.events == events) { // Idempotent insert
                  Future successful id
                } else {
                  val newId = generateId
                  if (newId == id || retries == 0) Future failed new DuplicateIdException(id, metadata)
                  else insertImpl(newId, generateId, tick, stEvt, metadata, retries - 1)
                }
            }
          committedId.foreach(_ => snapshots.write(id, new Snapshot(state, 0, tick)))
          committedId
        }
    }

  private def recordUpdate(id: RID, state: S, newRevision: Int, events: List[EVT], metadata: Map[String, String], tick: Long): Future[Int] = {
    val committedRevision = eventStore.commit(channel, id, newRevision, tick, events, metadata).map(_.revision)
    committedRevision.foreach(rev => snapshots.write(id, new Snapshot(state, rev, tick)))
    committedRevision
  }

  /**
   * Notification on every concurrent update collision.
   * This happens when two or more concurrent processes
   * attempt to update the same revision.
   * Should be rare, unless there is unusually high contention
   * on a specific stream, or if clients are offline and
   * working off stale data.
   * This does not affect correctness, as conflicts will be
   * retried until resolved.
   * Can be used for monitoring and reporting.
   */
  protected def onUpdateCollision(id: RID, revision: Int, channel: Channel): Unit = ()

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
                case dupe: eventStore.DuplicateRevisionException =>
                  val state = newState(snapshot.content)
                  dupe.conflict.events.foreach(state.mutate)
                  val latestSnapshot = Future successful Some(new Snapshot(state.curr, dupe.conflict.revision, dupe.conflict.tick))
                  onUpdateCollision(id, dupe.conflict.revision, dupe.conflict.channel)
                  loadAndUpdate(id, expectedRevision, metadata, latestSnapshot, true, updateThunk)
              }
            }
        }
    }
  }

  protected def update[_](
      expectedRevision: Option[Int], id: RID,
      updateThunk: (RepoType, Int) => Future[RepoType])(
      implicit
      metadata: Metadata): Future[Int] = try {
    loadAndUpdate(id, expectedRevision, metadata.toMap, snapshots.read(id), assumeCurrentSnapshots, updateThunk)
  } catch {
    case NonFatal(th) => Future failed th
  }

}
