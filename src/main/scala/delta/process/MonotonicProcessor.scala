package delta.process

import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag
import scuff.concurrent.AsyncStreamConsumer
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.collection.immutable.TreeMap
import java.util.concurrent.TimeoutException

import scala.concurrent._
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
    def isActive: Boolean
    def unapplied: Unapplied
    def promise: Promise[Unit]
  }
  /** Currently being processed. */
  private final class Active(val unapplied: Unapplied, val promise: Promise[Unit])
    extends StreamStatus { def isActive = true }
  /** Not currently being processed. */
  private final class Inactive(val unapplied: Unapplied, val promise: Promise[Unit])
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
  private def forceInactive(stream: ID, unapplied: Unapplied): Unit = {
    assert(unapplied.nonEmpty)
    val status = streamStatus(stream)
    val allUnapplied = unapplied ++ status.unapplied
    val inactive = new Inactive(allUnapplied, status.promise)
    if (!streamStatus.replace(stream, status, inactive)) {
      forceInactive(stream, allUnapplied)
    }
  }

  /** Set inactive. An empty list indicates success. */
  private def setInactive(stream: ID, stillUnapplied: Unapplied): (Unapplied, Future[Unit]) = {
    streamStatus(stream) match {
      case active: Active =>
        val promise = active.promise
        if (active.unapplied.isEmpty) {
          if (stillUnapplied.isEmpty) {
            if (streamStatus.remove(stream, active)) {
              promise.success(())
              Empty -> promise.future
            } else setInactive(stream, stillUnapplied)
          } else { // Still has unapplied
            if (streamStatus.replace(stream, active, new Inactive(stillUnapplied, promise))) {
              Empty -> promise.future
            } else setInactive(stream, stillUnapplied)
          }
        } else {
          if (streamStatus.replace(stream, active, new Active(Empty, promise))) {
            active.unapplied ++ stillUnapplied -> promise.future
          } else setInactive(stream, stillUnapplied)
        }
      case _ => ???
    }
  }

  private[this] val streamStatus = new TrieMap[ID, StreamStatus]

  private[process] case class IncompleteStream(id: ID, stillActive: Boolean)
  protected def incompleteStreams: Iterable[IncompleteStream] = streamStatus.map {
    case (id, status) => IncompleteStream(id, status.isActive)
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

    val upsertResult: Future[(Option[SnapshotUpdate], Unapplied)] =
      processStore.upsert(stream) { existingSnapshot =>
        applyTransactions(unapplied, existingSnapshot)
      }

    val inactiveFuture = upsertResult.flatMap {
      case (maybeUpdate, stillUnapplied) =>
        maybeUpdate.foreach(upd => onSnapshotUpdate(stream, upd))
        setInactive(stream, stillUnapplied) match {
          case (Empty, future) => future
          case (moreUnapplied, _) => upsertUntilInactive(stream, moreUnapplied)
        }
    }

    inactiveFuture.andThen {
      case Failure(th) =>
        forceInactive(stream, unapplied)
        ec.reportFailure(th)
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
      case te: TimeoutException =>
        val incompletes = incompleteStreams
        if (incompletes.exists(_.stillActive)) { // This is inherently racy, but if we catch an active stream, we can provide a more accurate error message.
          throw new IllegalStateException(s"Stream processing still active. Timeout of $completionTimeout is possibly too tight", te)
        } else {
          val incompleteIds = incompletes.map(_.id).mkString(", ")
          val errMsg = s"""Replay processing timed out after $completionTimeout, due to incomplete stream processing of ids: $incompleteIds
Possible causes:
    - Insufficient tick skew window. Resolve by increasing max tick skew.
    - Incomplete process store content, i.e. partial deletion has occurred. For side-effecting processes, resolve by restoring from backup. For pure processes, either resolve by restoring from backup, or restart processing from scratch."
"""
          throw new IllegalStateException(errMsg, te)
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
        val snapshots = cmap.iterator
          .filter(_._2.modified)
          .map(t => t._1 -> t._2.snapshot)
        persistReplayState(snapshots).andThen {
          case _ => cmap.clear()
        }
      }
  }
}
