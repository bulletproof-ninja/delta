package delta.write

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Success
import scala.util.control.NonFatal

import delta._
import EventStoreRepository._

import scuff.Reduction

import scala.annotation.nowarn

/**
 * [[delta.EventStore]]-based [[delta.write.Repository]] implementation.
 * @tparam SID Stream id type
 * @tparam EVT Repository event type
 * @tparam S Repository state type
 * @tparam ID Repository id type
 * @param channel The channel corresponding to this repository
 * @param wrapState State wrapper
 * @param exeCtx The internal execution context
 * @param snapshots Optional snapshot store
 * @param assumeCurrentRevisions Can snapshots from the snapshot store
 * be assumed to be current? I.e. are the snapshots available to all
 * running processes, or isolated to this process, and if the latter,
 * is this process exclusively snapshotting the available ids?
 * NOTE: This is a probabilistic performance switch and does not
 * affect correctness. When `true` reads
 * @param es The event store implementation
 * @param idConv Conversion function from repository id to stream id
 *
 * @note This is a performance switch. Regardless of setting, it will
 * not affect correctness.
 */
class EventStoreRepository[SID, EVT, S >: Null, ID](
  channel: delta.Channel,
  StateRef: S => StateRef[S, EVT],
  snapshots: SnapshotStore[ID, S] = SnapshotStore.empty[ID, S],
  assumeCurrentRevisions: Boolean = false)(
  es: EventStore[SID, _ >: EVT],
  exeCtx: ExecutionContext)(
  implicit
  idConv: ID => SID)
extends Repository[ID, Save[S, EVT]]
with ImmutableEntity {

  private implicit def ec = exeCtx

  private[this] val eventStore = es.asInstanceOf[EventStore[SID, EVT]]

  private type Snapshot = delta.Snapshot[S]
  private type Transaction = eventStore.Transaction
  type Loaded = EventStoreRepository.Loaded[S, EVT]

  protected def revision(loaded: Loaded): Revision = loaded.revision

  def exists(id: ID): Future[Option[Revision]] = eventStore.currRevision(id)

  private case class Builder(
      applyEventsAfter: Revision, stateRef: StateRef[S, EVT],
      concurrentUpdates: List[Transaction] = Nil, lastTxOrNull: Transaction = null)

  private def loadCurrentSnapshot(
      snapshot: Option[Snapshot],
      expectedRevision: Option[Revision],
      replayer: Reduction[Transaction, Future[Builder]] => Future[Builder])
      : Future[Option[Loaded]] = {

    val futureBuilt = replayer apply new Reduction[Transaction, Future[Builder]] {
      private[this] val lastSeenRevision = expectedRevision getOrElse Int.MaxValue
      @volatile private[this] var builder =
        Builder(snapshot.map(_.revision) getOrElse -1, StateRef(snapshot.map(_.state).orNull))
      def next(tx: Transaction): Unit = {
        val b = this.builder
        if (tx.revision > b.applyEventsAfter) {
          tx.events.foreach(b.stateRef.mutate)
        }
        val concurrentUpdates: List[Transaction] =
          if (tx.revision > lastSeenRevision) tx :: b.concurrentUpdates
          else b.concurrentUpdates
        this.builder = b.copy(concurrentUpdates = concurrentUpdates, lastTxOrNull = tx)
      }
      def result(): Future[Builder] = Future successful this.builder

    }
    futureBuilt.map {
      case Builder(_, _, _, null) => snapshot.map { snapshot =>
        new Loaded(snapshot.state, snapshot.revision, snapshot.tick, Nil)
      }
      case Builder(_, state, concurrentUpdates, lastTx) => Some {
        new Loaded(state.get, lastTx.revision, lastTx.tick, concurrentUpdates.reverse)
      }
    }
  }

  private def loadLatest(
      id: ID, snapshot: Future[Option[Snapshot]],
      assumeCurrentRevision: Boolean, expectedRevision: Option[Revision])
      : Future[Loaded] = {

    // Failed snapshot read should not prevent loading. Report and continue...
    val recoveredSnapshot = snapshot recover {
      case NonFatal(cause) =>
        ec reportFailure new IllegalStateException(
            s"Failed to load snapshot. Replaying all events instead.", cause)
        None
    }
    val futureState: Future[Option[Loaded]] =
      recoveredSnapshot.flatMap { maybeSnapshot =>
        maybeSnapshot match {
          case Some(snapshot) =>
            if (assumeCurrentRevision && expectedRevision.forall(_ <= snapshot.revision)) {
              Future successful Some(Loaded(snapshot.state, snapshot.revision, snapshot.tick, Nil))
            } else {
              val minRev = expectedRevision.map(math.min(_, snapshot.revision)) getOrElse snapshot.revision
              loadCurrentSnapshot(maybeSnapshot, expectedRevision, eventStore.replayStreamFrom(id, minRev + 1))
            }
          case None =>
            loadCurrentSnapshot(maybeSnapshot, expectedRevision, eventStore.replayStream(id))
        }
      }
    futureState.map(_.getOrElse(throw new UnknownIdException(id)))
  }

  final def load(id: ID): Future[Loaded] =
    loadLatest(id, snapshots.read(id), false, None)

  def insert(
      newId: => ID,
      entity: Entity)(
      implicit
      metadata: Metadata)
      : Future[ID] =
    if (entity.events.nonEmpty) {
      insertImpl(newId, newId, eventStore.ticker.nextTick(), entity.state, entity.events, metadata.toMap)
    } else Future failed {
      new IllegalStateException(s"Nothing to insert, $channel has no events.")
    }

  private def insertImpl(
      idCandidate: ID, generateId: => ID, tick: Tick,
      state: S, events: List[EVT], metadata: Map[String, String],
      retries: Int = 256): Future[ID] = {

    val committedId: Future[ID] =
      eventStore.commit(channel, idCandidate, 0, tick, events, metadata)
        .map(_ => idCandidate)
        .recoverWith {
          case dupe: eventStore.DuplicateRevisionException =>
            if (dupe.conflict.events == events) { // Allow idempotent insert
              Future successful idCandidate
            } else {
              val newCandidateId = generateId
              if (newCandidateId == idCandidate || retries == 0) {
                Future failed new DuplicateIdException(idCandidate, metadata)
              } else { // Try again
                insertImpl(newCandidateId, generateId, tick, state, events, metadata, retries - 1)
              }
            }
        }
    committedId
      .andThen {
        case Success(id) =>
          snapshots.write(id, new Snapshot(state, 0, tick))
      }

  }

  private def recordUpdate(
      id: ID, state: S, newRevision: Revision, events: List[EVT],
      metadata: Map[String, String], tick: Tick)
      : Future[Revision] = {
    eventStore
      .commit(channel, id, newRevision, tick, events, metadata)
      .map(tx => tx.revision)
      .andThen {
        case Success(revision) =>
          snapshots.write(id, new Snapshot(state, revision, tick))
      }
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
  @nowarn
  protected def onUpdateCollision(id: ID, revision: Revision, channel: Channel): Unit = ()

  private def loadAndUpdate(
      id: ID, expectedRevision: Option[Revision],// metadata: Map[String, String],
      maybeSnapshot: Future[Option[Snapshot]], assumeCurrentRevision: Boolean,
      updateThunk: Loaded => Future[(Entity, Metadata)]): Future[Revision] = {

    loadLatest(id, maybeSnapshot, assumeCurrentRevision, expectedRevision)
      .flatMap { loaded =>
        updateThunk(loaded).flatMap {
          case (Save(newState, newEvents), metadata) =>
            if (newEvents.isEmpty || loaded.concurrentUpdates.flatMap(_.events) == newEvents) {
              Future successful loaded.revision
            } else {
              val tick = eventStore.ticker.nextTick(loaded.tick)
              recordUpdate(id, newState, loaded.revision + 1, newEvents, metadata.toMap, tick).recoverWith {
                case dupe: eventStore.DuplicateRevisionException =>
                  val stateRef = StateRef(loaded.state)
                  dupe.conflict.events.foreach(stateRef.mutate)
                  val latestSnapshot = Future successful Some {
                    new Snapshot(stateRef.get, dupe.conflict.revision, dupe.conflict.tick)
                  }
                  onUpdateCollision(id, dupe.conflict.revision, dupe.conflict.channel)
                  loadAndUpdate(id, expectedRevision, /*metadata,*/ latestSnapshot, true, updateThunk)
              }
            }
        }
    }
  }

  protected def update(
      updateThunk: Loaded => Future[UpdateReturn],
      id: ID, expectedRevision: Option[Revision])
      // (
      // implicit
      // metadata: Metadata)
      : Future[Revision] =
    try {
      loadAndUpdate(id, expectedRevision, /*metadata.toMap,*/ snapshots.read(id), assumeCurrentRevisions, updateThunk)
    } catch {
      case NonFatal(th) => Future failed th
    }

}

private[write] object EventStoreRepository {

  final case class Loaded[S, EVT](
    state: S, revision: Revision, tick: Tick,
    concurrentUpdates: List[delta.Transaction[_, EVT]])

  final case class Save[S, EVT](state: S, events: List[EVT])

}
