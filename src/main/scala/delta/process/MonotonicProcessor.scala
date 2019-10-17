package delta.process

import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag
import scala.util.Failure
import scala.collection.immutable.TreeMap
import java.util.concurrent.TimeoutException

import scuff.concurrent._
import scala.concurrent._, duration.FiniteDuration
import delta._

private object MonotonicProcessor {

  implicit class UnappliedOps[TXN <: Transaction[_, _]](private val map: TreeMap[Int, TXN]) extends AnyVal {
    @inline def add(txn: TXN): TreeMap[Int, TXN] = map.updated(txn.revision, txn)
  }

}

trait MonotonicProcessor[ID, EVT, S >: Null]
  extends TransactionProcessor[ID, EVT, S]
  with (Transaction[ID, _ >: EVT] => Future[Unit]) {

  type Snapshot = delta.Snapshot[S]
  type SnapshotUpdate = delta.process.SnapshotUpdate[S]

  protected def processStore: StreamProcessStore[ID, S]
  protected def processContext(id: ID): ExecutionContext

  import MonotonicProcessor.UnappliedOps
  private type Revision = Int
  private[this] val Empty = TreeMap.empty[Revision, TXN]
  private type Unapplied = TreeMap[Revision, TXN]

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
  private def setActive(txn: TXN): (Unapplied, Future[Unit]) = {
    streamStatus.get(txn.stream) match {
      case None =>
        val promise = Promise[Unit]
        val active = new Active(Empty, promise)
        if (streamStatus.putIfAbsent(txn.stream, active).isEmpty) {
          Empty.add(txn) -> promise.future
        } else setActive(txn)
      case Some(inactive: Inactive) =>
        val promise = inactive.promise
        val active = new Active(Empty, promise)
        if (streamStatus.replace(txn.stream, inactive, active)) {
          inactive.unapplied.add(txn) -> promise.future
        } else setActive(txn)
      case Some(active: Active) =>
        val promise = active.promise
        val newActive = new Active(active.unapplied.add(txn), promise)
        if (streamStatus.replace(txn.stream, active, newActive)) {
          Empty -> promise.future
        } else setActive(txn)
    }
  }
  private def forceInactive(stream: ID, unapplied: Unapplied, cause: Throwable): Unit = {
    assert(unapplied.nonEmpty)
    val status = streamStatus(stream)
    val allUnapplied = unapplied ++ status.unapplied
    val inactive = new Inactive(allUnapplied, status.promise)
    if (streamStatus.replace(stream, status, inactive)) {
      inactive.promise tryFailure cause
    } else {
      forceInactive(stream, allUnapplied, cause)
    }
  }

  /** Set inactive. An empty list indicates success. */
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
      id: ID, stillActive: Boolean, expectedRevision: Revision, unappliedRevisions: Range,
      status: Future[Unit])
  protected def incompleteStreams: Iterable[IncompleteStream] = streamStatus.map {
    case (id, status) => IncompleteStream(id, status.isActive, status.revision + 1, status.unapplied.head._1 to status.unapplied.last._1, status.promise.future)
  }

  protected def onSnapshotUpdate(id: ID, update: SnapshotUpdate): Unit
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
      val txn = unapplied.head._2
      if (txn.revision == expectedRev) {
        callProcess(txn, snapshot.map(_.content)).flatMap { updated =>
          val tick = snapshot.map(_.tick max txn.tick) getOrElse txn.tick
          applyTransactions(unapplied.tail, Some(Snapshot(updated, txn.revision, tick)))
        }
      } else if (txn.revision < expectedRev) { // Already processed
        applyTransactions(unapplied.tail, snapshot)
      } else { // txn.revision > expectedRev
        onMissingRevisions(txn.stream, expectedRev until txn.revision)
        Future successful (snapshot -> unapplied)
      }
    }

  }

  private def upsertUntilInactive(
      stream: ID, unapplied: Unapplied)(
      implicit
      ec: ExecutionContext): Future[Unit] = {

    val upsertResult =
      processStore.upsert(stream) { existingSnapshot =>
        applyTransactions(unapplied, existingSnapshot).map {
          case (latestSnapshot, stillUnapplied) =>
            latestSnapshot -> (stillUnapplied -> (latestSnapshot.map(_.revision) getOrElse -1))
        }
      }

    val inactiveFuture = upsertResult.flatMap {
      case (maybeUpdate, (stillUnapplied, currRevision)) =>
        maybeUpdate.foreach(onSnapshotUpdate(stream, _))
        setInactive(stream, stillUnapplied, currRevision) match {
          case (Empty, future) => future
          case (moreUnapplied, _) => upsertUntilInactive(stream, moreUnapplied)
        }
    }

    inactiveFuture.andThen {
      case Failure(cause) =>
        forceInactive(stream, unapplied, cause)
        ec.reportFailure(cause)
    }
  }

  def apply(txn: TXN): Future[Unit] = {
    setActive(txn) match {
      case (Empty, future) => future
      case (unapplied, _) =>
        upsertUntilInactive(txn.stream, unapplied)(processContext(txn.stream))
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
abstract class MonotonicReplayProcessor[ID, EVT, S >: Null, BR](
    finishProcessingTimeout: FiniteDuration,
    protected val processStore: StreamProcessStore[ID, S])(
    implicit
    protected val evtTag: ClassTag[EVT])
  extends MonotonicProcessor[ID, EVT, S]
  with AsyncStreamConsumer[Transaction[ID, _ >: EVT], BR] {

  protected final def completionTimeout = finishProcessingTimeout

  override def onDone(): Future[BR] = {
    // Reminder: Once `super.onDone()` future completes, `whenDone()` has already run.
    super.onDone().recover {
      case timeout: TimeoutException =>
        val incompletes = incompleteStreams
        if (incompletes.exists(_.stillActive)) { // This is inherently racy, but if we catch an active stream, we can provide a more accurate error message.
          throw new IllegalStateException(s"Stream processing still active. Timeout of $completionTimeout is possibly too tight", timeout)
        } else {
          val cause = incompletes
            .map(_.status.failed)
            .find(_.isCompleted)
            .map(_.value.get.get)
            .getOrElse(timeout)
          val firstIncomplete = incompletes.headOption.map { incomplete =>
            val unapplied = incomplete.unappliedRevisions
            val missing = incomplete.expectedRevision until unapplied.start
            val textMissing = if (incomplete.status.failed.isCompleted) {
              s"nothing, but failed processing. See stack trace below."
            } else if (missing.isEmpty) {
              s"... nothing? Expecting revision ${incomplete.expectedRevision}, but have revisions ${unapplied.start}-${unapplied.last} unapplied, yet inactive."
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
    - Insufficient tick skew window. Resolve by increasing max tick skew.
    - Incomplete process store content, i.e. partial deletion has occurred. For side-effecting processes, resolve by restoring from backup. For pure processes, either resolve by restoring from backup, or restart processing from scratch."
"""
          throw new IllegalStateException(errMsg, cause)
        }
    }(scuff.concurrent.Threads.PiggyBack)
  }

  /** Don't broadcast snapshot updates during replay. */
  protected def onSnapshotUpdate(id: ID, update: SnapshotUpdate): Unit = ()

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

trait ConcurrentMapReplayPersistence[ID, EVT, S >: Null, BR] {
  proc: MonotonicReplayProcessor[ID, EVT, S, BR] =>

  protected type Value = ConcurrentMapStore.Value[S]

  /** Execution context for persisting final replay state. */
  protected def persistenceContext: ExecutionContext
  protected def persistReplayState(snapshots: Iterator[(ID, Snapshot)]): Future[BR]
  protected def onReplayCompletion(): Future[collection.concurrent.Map[ID, Value]]
  protected def whenDone(): Future[BR] = {
      implicit def ec = persistenceContext
    onReplayCompletion()
      .flatMap { cmap =>
        require(incompleteStreams.isEmpty, s"Incomplete streams are present. This code should not execute. This is a bug.")
        val snapshots = cmap.iterator.collect {
          case (id, ConcurrentMapStore.Value(snapshot, true)) => id -> snapshot
        }
        persistReplayState(snapshots).andThen {
          case _ => cmap.clear()
        }
      }
  }
}
