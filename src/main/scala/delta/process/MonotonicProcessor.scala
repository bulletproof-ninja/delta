package delta.process

import scala.collection.concurrent.TrieMap
import scala.util.Failure
import scala.collection.immutable.TreeMap
import java.util.concurrent.TimeoutException

import scala.concurrent._, duration.FiniteDuration
import delta._
import scala.annotation.tailrec
import scala.util.Try
import scuff.concurrent._

private object MonotonicProcessor {

  implicit class UnappliedOps[TX <: Transaction[_, _]](private val map: TreeMap[Int, TX]) extends AnyVal {
    @inline def add(tx: TX): TreeMap[Int, TX] = map.updated(tx.revision, tx)
  }

}

/**
 * Monotonic replay processor.
 * @tparam ID Stream identifier type
 * @tparam EVT Event type
 * @tparam S State type
 */
trait MonotonicProcessor[ID, EVT, S >: Null, U]
  extends TransactionProcessor[ID, EVT, S]
  with (Transaction[ID, _ >: EVT] => Future[Unit]) {

  type Snapshot = delta.Snapshot[S]
  type Update = delta.process.Update[U]

  protected def processStore: StreamProcessStore[ID, S, U]
  protected def processContext(id: ID): ExecutionContext

  import MonotonicProcessor.UnappliedOps
  private type Revision = Int
  private[this] val Empty = TreeMap.empty[Revision, Transaction]
  private type Unapplied = TreeMap[Revision, Transaction]

  sealed private abstract class StreamStatus {
    def revision: Revision
    def isActive: Boolean
    def unapplied: Unapplied
    def promise: Promise[Unit]
  }
  /** Currently being processed. */
  private final class Active(val unapplied: Unapplied, val promise: Promise[Unit], val revision: Revision = -1)
    extends StreamStatus { def isActive = true }
  /** Not currently being processed. */
  private final class Inactive(val unapplied: Unapplied, val promise: Promise[Unit], val revision: Revision = -1)
    extends StreamStatus { def isActive = false }

  /** Set active. A non-empty map indicates success. */
  private def setActive(tx: Transaction): (Unapplied, Future[Unit]) = {
    streamStatus.get(tx.stream) match {
      case None =>
        val promise = Promise[Unit]
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
  private def setInactive(stream: ID, stillUnapplied: Unapplied, currRevision: Int): (Unapplied, Future[Unit]) = {
    streamStatus(stream) match {
      case active: Active =>
        val promise = active.promise
        if (active.unapplied.isEmpty) {
          if (stillUnapplied.isEmpty) {
            if (streamStatus.remove(stream, active)) {
              promise.success(())
              Empty -> promise.future
            } else setInactive(stream, stillUnapplied, currRevision)
          } else { // Still has unapplied
            if (streamStatus.replace(stream, active, new Inactive(stillUnapplied, promise, currRevision))) {
              Empty -> promise.future
            } else setInactive(stream, stillUnapplied, currRevision)
          }
        } else {
          if (streamStatus.replace(stream, active, new Active(Empty, promise, currRevision))) {
            active.unapplied ++ stillUnapplied -> promise.future
          } else setInactive(stream, stillUnapplied, currRevision)
        }
      case _ => ???
    }
  }

  private[this] val streamStatus = new TrieMap[ID, StreamStatus]

  private[process] case class IncompleteStream(
      id: ID, stillActive: Boolean, expectedRevision: Revision, unappliedRevisions: Option[Range],
      status: Future[Unit])
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

  /**
   * @return Most current snapshot and any unapplied transactions
   */
  private def applyTransactions(
      unapplied: Unapplied, snapshot: Option[Snapshot])(
      implicit
      ec: ExecutionContext): Future[(Option[Snapshot], Unapplied)] = {
    if (unapplied.isEmpty) {
      Future successful (snapshot -> unapplied)
    } else {
      val expectedRev = snapshot.map(_.revision + 1) getOrElse 0
      val tx = unapplied.head._2
      if (tx.revision == expectedRev) {
        callProcess(tx, snapshot.map(_.content)).flatMap { newState =>
          val tick = snapshot.map(_.tick max tx.tick) getOrElse tx.tick
          applyTransactions(unapplied.tail, Some(Snapshot(newState, tx.revision, tick)))
        }
      } else if (tx.revision < expectedRev) { // Already processed
        applyTransactions(unapplied.tail, snapshot)
      } else { // tx.revision > expectedRev
        onMissingRevisions(tx.stream, expectedRev until tx.revision)
        Future successful (snapshot -> unapplied)
      }
    }

  }

  private def upsertUntilInactive(
      stream: ID, unapplied: Unapplied)(
      implicit
      ec: ExecutionContext): Future[Unit] = {

    val upsertResult: Future[(Option[Update], Unapplied)] =
      processStore.upsert(stream) { existingSnapshot =>
        applyTransactions(unapplied, existingSnapshot)
      }

    val inactiveFuture = upsertResult.flatMap {
      case (maybeUpdate, stillUnapplied) =>
        Try(maybeUpdate.foreach(onUpdate(stream, _))).failed.foreach(ec.reportFailure)
        val currRevision = maybeUpdate.map(_.revision) getOrElse -1
        setInactive(stream, stillUnapplied, currRevision) match {
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

  def apply(tx: Transaction): Future[Unit] = {
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
 * @tparam BR Batch result. Typically just `Unit`
 */
abstract class MonotonicReplayProcessor[ID, EVT, S >: Null, U, BR](
  finishProcessingTimeout: FiniteDuration,
  protected val processStore: StreamProcessStore[ID, S, U])
extends MonotonicProcessor[ID, EVT, S, U]
with AsyncStreamConsumer[Transaction[ID, _ >: EVT], BR] {

  protected final def completionTimeout = finishProcessingTimeout

  override def onDone(): Future[BR] = {
    // Reminder: Once `super.onDone()` future completes, `whenDone()` has already run.
    super.onDone().recover {
      case timeout: TimeoutException =>
        val incompletes = incompleteStreams
        if (incompletes.exists(_.stillActive)) { // This is inherently racy, but if we catch an active stream, we can provide a more accurate error message.
          throw new IllegalStateException(s"Stream processing still active. Timeout of $completionTimeout is too tight", timeout)
        } else {
          val cause = incompletes
            .map(_.status.failed)
            .find(_.isCompleted)
            .map(_.value.get.get)
            .getOrElse(timeout)
          val firstIncomplete = incompletes.headOption.map { incomplete =>
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
            s"First one, stream ${incomplete.id}, is missing $textMissing"
          } getOrElse "(unable to elaborate)"
          val incompleteIds = incompletes.map(_.id).mkString(", ")
          val errMsg = s"""Replay processing timed out after $completionTimeout, due to incomplete stream processing of ids: $incompleteIds
$firstIncomplete
Possible causes:
    - Insufficient tick window. Resolve by increasing max tick skew.
    - Incomplete process store content; possible causes:
        - An earlier attempt at replay persistence was interrupted or killed
        - Replay persistence actively running on the same data set
        - Partial/incomplete deletion of entries.
      For side-effecting processes, resolve by restoring from backup.
      For pure processes, either resolve by restoring from backup, or restart processing from scratch.
"""
          throw new IllegalStateException(errMsg, cause)
        }
    }(scuff.concurrent.Threads.PiggyBack)
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
   *  Called when replay processing is done.
   *  This is the time to persist all generated
   *  state, and return any information that
   *  will be handed over to the live
   *  processor. Should generally only be concerned
   *  with a successful `Future`.
   */
  protected def whenDone(): Future[BR]
}

trait ConcurrentMapReplayPersistence[ID, EVT, S >: Null, U, BR] {
  proc: MonotonicReplayProcessor[ID, EVT, S, U, BR] =>

  type Snapshot = delta.Snapshot[S]
  protected type State = ConcurrentMapStore.State[S]

  /** Execution context for persisting final replay state. */
  protected def persistenceContext: ExecutionContext
  protected def persistReplayState(snapshots: Iterator[(ID, Snapshot)]): Future[BR]
  protected def onReplayCompletion(): Future[collection.concurrent.Map[ID, State]]
  protected def whenDone(): Future[BR] = {
      implicit def ec = persistenceContext
    onReplayCompletion()
      .flatMap { cmap =>
        require(incompleteStreams.isEmpty, s"Incomplete streams are present. This code should not execute. This is a bug.")
        val snapshots = cmap.iterator.collect {
          case (id, value) if value.modified => id -> value.snapshot
        }
        persistReplayState(snapshots).andThen {
          case _ => cmap.clear()
        }
      }
  }
}
