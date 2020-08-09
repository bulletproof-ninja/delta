package delta.process

import scala.collection.concurrent.TrieMap
import scala.util.Failure
import scala.collection.immutable.TreeMap
import java.util.concurrent.TimeoutException

import scala.concurrent._, duration.FiniteDuration
import delta.{ Snapshot, Transaction, Revision }
import scala.annotation.tailrec
import scuff.concurrent._
import scala.util.control.NonFatal
import java.{util => ju}

private object MonotonicProcessor {

  implicit class UnappliedOps[TX <: Transaction[_, _]](private val map: TreeMap[Int, TX]) extends AnyVal {
    @inline def add(tx: TX): TreeMap[Int, TX] = map.updated(tx.revision, tx)
  }

  private[process] val tickComparator = new ju.Comparator[(Any, Snapshot[Any])] {
    def compare(o1: (Any, Snapshot[Any]), o2: (Any, Snapshot[Any])): Int =
      (o1._2.tick - o2._2.tick).toInt
  }

}

/**
 * Monotonic replay processor.
 * @tparam ID Stream identifier type
 * @tparam EVT Event type
 * @tparam S State type
 */
trait MonotonicProcessor[ID, EVT, S >: Null, U]
extends (Transaction[ID, _ >: EVT] => Future[S])
with TransactionProcessor[ID, EVT, S] {

  type Snapshot = delta.Snapshot[S]
  type Update = delta.process.Update[U]

  protected def processStore: StreamProcessStore[ID, S, U]
  protected def processContext(id: ID): ExecutionContext

  import MonotonicProcessor.UnappliedOps
  private[this] val Empty = TreeMap.empty[Revision, Transaction]
  private type Unapplied = TreeMap[Revision, Transaction]

  sealed private abstract class StreamStatus {
    def revision: Revision
    def isActive: Boolean
    def unapplied: Unapplied
    def promise: Promise[S]
  }
  /** Currently being processed. */
  private final class Active(val unapplied: Unapplied, val promise: Promise[S], val revision: Revision = -1)
    extends StreamStatus { def isActive = true }
  /** Not currently being processed. */
  private final class Inactive(val unapplied: Unapplied, val promise: Promise[S], val revision: Revision = -1)
    extends StreamStatus { def isActive = false }

  /** Set active. A non-empty map indicates success. */
  private def setActive(tx: Transaction): (Unapplied, Future[S]) = {
    streamStatus.get(tx.stream) match {
      case None =>
        val promise = Promise[S]()
        val active = new Active(Empty, promise)
        if (streamStatus.putIfAbsent(tx.stream, active).isEmpty) {
          Empty.add(tx) -> promise.future
        } else setActive(tx)
      case Some(inactive: Inactive) =>
        val promise = inactive.promise
        val active = new Active(Empty, promise)
        if (streamStatus.replace(tx.stream, inactive, active)) {
          inactive.unapplied.add(tx) -> promise.future
        } else setActive(tx)
      case Some(active: Active) =>
        val promise = active.promise
        val newActive = new Active(active.unapplied.add(tx), promise)
        if (streamStatus.replace(tx.stream, active, newActive)) {
          Empty -> promise.future
        } else setActive(tx)
    }
  }

  @tailrec
  private def ensureInactive(stream: ID, unapplied: Unapplied, cause: Throwable): Unit = {
    streamStatus.get(stream) match {
      case None => // Already processed, thus inactive
      case Some(status) =>
        val allUnapplied = unapplied ++ status.unapplied
        val inactive = new Inactive(allUnapplied, status.promise)
        if (streamStatus.replace(stream, status, inactive)) {
          inactive.promise tryFailure cause
        } else {
          ensureInactive(stream, allUnapplied, cause)
        }
    }
  }

  /** Set inactive. An empty list indicates success. */
  @tailrec
  private def setInactive(
      stream: ID, procState: ProcessingState, currRevision: Revision): (Unapplied, Future[S]) = {
    streamStatus(stream) match {
      case active: Active =>
        val promise = active.promise
        if (active.unapplied.isEmpty) {
          procState match {
            case Finished(snapshot) =>
              if (streamStatus.remove(stream, active)) {
                promise success snapshot.state
                Empty -> promise.future
              } else setInactive(stream, procState, currRevision)
            case _ => // Still has unapplied
              if (streamStatus.replace(stream, active, new Inactive(procState.unapplied, promise, currRevision))) {
                Empty -> promise.future
              } else setInactive(stream, procState, currRevision)

          }
        } else {
          if (streamStatus.replace(stream, active, new Active(Empty, promise, currRevision))) {
            active.unapplied ++ procState.unapplied -> promise.future
          } else setInactive(stream, procState, currRevision)
        }
      case _ => ???
    }
  }

  private[this] val streamStatus = new TrieMap[ID, StreamStatus]

  private[process] case class IncompleteStream(
      id: ID, stillActive: Boolean, expectedRevision: Revision, unappliedRevisions: Option[Range],
      status: Future[S])
  protected def incompleteStreams: Iterable[IncompleteStream] = streamStatus.map {
    case (id, status) =>
      val unappliedRevisions = for {
        (first, _) <- status.unapplied.headOption
        (last, _) <- status.unapplied.lastOption
      } yield {
        first to last
      }
      IncompleteStream(id, status.isActive, status.revision + 1, unappliedRevisions, status.promise.future)
  }

  protected def onUpdate(id: ID, update: Update): Unit
  protected def onMissingRevisions(id: ID, missing: Range): Unit

  private sealed abstract class ProcessingState {
    def unapplied: Unapplied
    def optSnapshot: Option[Snapshot]
    def snapshotState: Option[S]
    def expectedRevision: Revision
    def maxTick(alt: Long): Long
  }
  private object ProcessingState {
    def apply(snapshot: Snapshot, unapplied: Unapplied): ProcessingState =
      if (unapplied.isEmpty) new Finished(snapshot)
      else new Intermediate(snapshot, unapplied)
    def apply(snapshot: Option[Snapshot], unapplied: Unapplied): ProcessingState =
      snapshot match {
        case Some(snapshot) => this.apply(snapshot, unapplied)
        case _ => Beginning(unapplied)
      }
  }
  private case class Beginning(
    unapplied: Unapplied)
  extends ProcessingState { assert(unapplied.nonEmpty)
    def optSnapshot: Option[Snapshot] = None
    def snapshotState = None
    def expectedRevision = 0
    def maxTick(alt: Long) = alt
  }
  private case class Intermediate(
    snapshot: Snapshot, unapplied: Unapplied)
  extends ProcessingState { assert(unapplied.nonEmpty)
    def optSnapshot: Option[Snapshot] = Some(snapshot)
    def snapshotState = Some(snapshot.state)
    def expectedRevision = snapshot.revision + 1
    def maxTick(alt: Long) = snapshot.tick max alt
  }
  private case class Finished(
    snapshot: Snapshot)
  extends ProcessingState {
    def unapplied: Unapplied = Empty
    def optSnapshot: Option[Snapshot] = Some(snapshot)
    def snapshotState = Some(snapshot.state)
    def expectedRevision = snapshot.revision + 1
    def maxTick(alt: Long) = snapshot.tick max alt
  }

  /**
   * @return Most current snapshot and any unapplied transactions
   */
  private def applyTransactions(
      procState: ProcessingState)(
      implicit
      ec: ExecutionContext): Future[ProcessingState] = {

    if (procState.unapplied.isEmpty) {
      Future successful procState
    } else {
      val snapshotState = procState.snapshotState
      val expectedRev = procState.expectedRevision
      val tx = procState.unapplied.head._2
      if (tx.revision == expectedRev) {
        callProcess(tx, snapshotState).flatMap { newState =>
          val tick = procState maxTick tx.tick
          val snapshot = Snapshot(newState, tx.revision, tick)
          applyTransactions(ProcessingState(snapshot, procState.unapplied.tail))
        }
      } else if (tx.revision < expectedRev) { // Already processed
        applyTransactions(ProcessingState(procState.optSnapshot, procState.unapplied.tail))
      } else { // tx.revision > expectedRev
        onMissingRevisions(tx.stream, expectedRev until tx.revision)
        Future successful ProcessingState(procState.optSnapshot, procState.unapplied)
      }
    }

  }

  private def upsertUntilInactive(
      stream: ID, unapplied: Unapplied)(
      implicit
      ec: ExecutionContext): Future[S] = {

    val upsertResult: Future[(Option[Update], ProcessingState)] =
      processStore.upsert(stream) { existingSnapshot =>
        applyTransactions(ProcessingState(existingSnapshot, unapplied))
          .map { procState =>
            procState.optSnapshot -> procState
          }(Threads.PiggyBack)
      }

    val inactiveFuture = upsertResult.flatMap {
      case (maybeUpdate, procState) =>
        val currRevision = maybeUpdate match {
          case Some(update) =>
            try onUpdate(stream, update) catch {
              case NonFatal(cause) => ec reportFailure cause
            }
            update.revision
          case _ => -1
        }
        setInactive(stream, procState, currRevision) match {
          case (Empty, future) => future
          case (moreUnapplied, _) => upsertUntilInactive(stream, moreUnapplied)
        }
    }

    inactiveFuture.andThen {
      case Failure(cause) => // Unknown cause, but most likely from applying Transaction
        try ec.reportFailure(cause) finally {
          ensureInactive(stream, unapplied, cause)
        }
    }
  }

  def apply(tx: Transaction): Future[S] = {
    setActive(tx) match {
      case (Empty, future) => future
      case (unapplied, _) =>
        upsertUntilInactive(tx.stream, unapplied)(processContext(tx.stream))
    }

  }
}

/**
 * Monotonic replay processor.
 * @see delta.process.EventSourceProcessor
 * @tparam ID Stream identifier
 * @tparam EVT Event type
 * @tparam S State type
 * @tparam BR Batch result. Often just `Unit`
 */
abstract class MonotonicReplayProcessor[ID, EVT, S >: Null, U, BR](
  postReplayTimeout: FiniteDuration,
  protected val processStore: StreamProcessStore[ID, S, U])(
  implicit protected val executionContext: ExecutionContext)
extends MonotonicProcessor[ID, EVT, S, U]
with AsyncStreamConsumer[Transaction[ID, _ >: EVT], BR] {

  private val instanceName = s"${getClass.getSimpleName}(${processStore.name})"
  override def toString() = s"$instanceName@${hashCode}"

  protected final def completionTimeout = postReplayTimeout

  override def onDone(): Future[BR] =
    // Reminder: Once `super.onDone()` future completes, `whenDone()` has already run.
    super.onDone().recover {
      case timeout: TimeoutException =>
        val incompletes = incompleteStreams
        val activeCount = incompletes.count(_.stillActive)
        if (activeCount > 0) { // This is inherently racy, but if we catch an active stream, we can provide a more definite error message.
          throw new IllegalStateException(s"$instanceName has $activeCount unfinished transactions. Timeout of $completionTimeout is too short! Increase timeout and try again", timeout)
        } else {
          val cause = incompletes
            .map(_.status.failed)
            .find(_.isCompleted)
            .map(_.value.get.get)
            .getOrElse(timeout)
          val firstIncomplete =
            incompletes.headOption.map { incomplete =>
              val unapplied = incomplete.unappliedRevisions
              val unappliedStart = unapplied.map(_.start)
              val missing = incomplete.expectedRevision until unappliedStart.getOrElse(incomplete.expectedRevision)
              val textMissing = if (incomplete.status.failed.isCompleted) {
                s"nothing, but failed processing. See stack trace below."
              } else if (missing.isEmpty) {
                val unappliedMsg = unapplied match {
                  case Some(unapplied) => s"revisions ${unapplied.start}-${unapplied.last} unapplied"
                  case None => s"all revisions applied"
                }
                s"... nothing? Expecting revision ${incomplete.expectedRevision}, but have $unappliedMsg, yet inactive."
              } else if (missing.start == missing.last) {
                s"revision ${missing.start}"
              } else {
                s"revisions ${missing.start}-${missing.last}"
              }
              s"E.g. first one, stream ${incomplete.id}, is missing $textMissing"
            } getOrElse "(unable to elaborate)"
          val incompleteIds = incompletes.map(_.id).mkString(", ")
          val errMsg = s"""$instanceName replay processing timed out after $completionTimeout, due to incomplete stream processing of ids: $incompleteIds
$firstIncomplete
Possible causes:
    - Insufficient tick window. Resolve by increasing tick window.
    - Incomplete process store content; possible causes:
        - An earlier replay attempt was interrupted or killed during persistence phase.
        - Another replay persistence process actively running on the same data set.
        - Partial/incomplete deletion of entries.
Possible solutions:
    - When process is pure (no side effects), or has idempotent side effects:
        - Delete dataset and restart this process, or
        - Restore from backup.
    - When process has non-idempotent side effects (impure):
        - Restore from backup, or
        - Delete dataset and restart this process AND DEAL WITH THE REPEATED SIDE EFFECTS
"""
          throw new IllegalStateException(errMsg, cause)
        }
    }

  /** Don't broadcast snapshot updates during replay. */
  protected def onUpdate(id: ID, update: Update): Unit = ()

  /**
   * Missing revision replay is disabled on replay, since
   * the stream will very likely be out of order, and it's
   * considered complete once done.
   */
  protected def onMissingRevisions(id: ID, missing: Range): Unit = ()

  /**
   * Called when replay processing is successfully
   * completed.
   * This is the time to persist all generated
   * state, and return any information that
   * will be handed over to the live
   * processor.
   */
  protected def whenDone(): Future[BR]
}

/**
  * Replay persistence coordination, expecting a
  * `concurrent.Map` to hold replay state.
  */
trait ConcurrentMapReplayPersistence[ID, EVT, S >: Null, U, BR] {
  proc: MonotonicReplayProcessor[ID, EVT, S, U, BR] =>

  type Snapshot = delta.Snapshot[S]
  protected type State = ConcurrentMapStore.State[S]

  /**
    * Persist snapshots from completed replay.
    * @note This is a critical step. If persistence is interrupted,
    * and it's not persisted in tick order (default), it can lead to
    * an invalid store state, which will fail upon restart.
    * To prevent this, clear the store before restart
    * *OR* persist in ascending tick order (default).
    *
    * @see persistInTickOrder To persist in tick order. Defaults to `true`
    *
    * @param snapshots
    */
  protected def persistReplayState(snapshots: Iterator[(ID, Snapshot)]): Future[BR]
  protected def onReplayCompletion(): Future[collection.concurrent.Map[ID, State]]

  /**
    * When replay processing is done, should persistence
    * happen in ascending tick order?
    *
    * If `true`, then a copy of the data set is made and then
    * sorted. This prevents certain edge cases when processes are
    * killed during persistence, at the cost of transient increased
    * memory usage.
    *
    * If `false`, then the data set is persisted in arbitrary order.
    * This means that if the persistence phase is interrupted, the
    * process store is in an invalid state and should be cleared
    * before restarting.
    *
    * @note Defaults to `true`
    */
  protected def persistInTickOrder: Boolean = true

  protected def whenDone(): Future[BR] = {
    onReplayCompletion()
      .flatMap { cmap =>

        require(
          incompleteStreams.isEmpty,
          s"Incomplete streams are present. This code should not execute. This is a bug.")

        val snapshots = cmap.iterator.collect {
          case (id, value) if value.modified => id -> value.snapshot
        }
        if (persistInTickOrder) {
          val array = try snapshots.toArray catch {
            case _: OutOfMemoryError =>
              throw new OutOfMemoryError(
                  s"Increase available memory, or override `persistInTickOrder` to `false`")
          }
          cmap.clear()
          ju.Arrays.sort(array, MonotonicProcessor.tickComparator)
          persistReplayState(array.iterator)
        } else persistReplayState(snapshots).andThen {
          case _ => cmap.clear()
        }
      }
  }
}
