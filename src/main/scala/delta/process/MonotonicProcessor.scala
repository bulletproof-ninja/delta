package delta.process

import scala.collection.concurrent.TrieMap
import scala.util.Failure
import scala.collection.immutable.TreeSet
import java.util.concurrent.TimeoutException

import scala.concurrent._
import delta.{ Snapshot, Transaction, Revision, Tick }
import scala.annotation.tailrec
import scuff.concurrent._
import scala.util.control.NonFatal
import scala.util._
import java.util.Arrays

private object MonotonicProcessor {

  private[this] val txRevOrdering = Ordering.by[Transaction[Any, Any], Int](_.revision)

  private def TxRevOrdering[ID] =
    txRevOrdering.asInstanceOf[Ordering[Transaction[ID, Any]]]

  private[this] val tickComparator = new java.util.Comparator[(Any, Snapshot[Any])] {
    def compare(o1: (Any, Snapshot[Any]), o2: (Any, Snapshot[Any])): Int =
      (o1._2.tick - o2._2.tick).toInt
  }

  private[process] def TickComparator[ID, S] = tickComparator.asInstanceOf[java.util.Comparator[(ID, Snapshot[S])]]

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

  def name = processStore.name
  protected def processStore: StreamProcessStore[ID, S, U]
  protected def processContext(id: ID): ExecutionContext

  import MonotonicProcessor._
  private[this] val Empty = TreeSet.empty[Transaction](TxRevOrdering[ID])
  private type Unapplied = TreeSet[Transaction]

  sealed private abstract class StreamStatus {
    def revision: Revision
    def isActive: Boolean
    def unapplied: Unapplied
    def promise: Promise[S]
    def isFailure: Boolean =
      promise.isCompleted &&
      promise.future.value.get.isFailure
  }
  /** Currently being processed. */
  private final class Active(val unapplied: Unapplied, val promise: Promise[S], val revision: Revision)
    extends StreamStatus { def isActive = true }
  /** Not currently being processed. */
  private final class Inactive(val unapplied: Unapplied, val promise: Promise[S], val revision: Revision)
    extends StreamStatus { def isActive = false }

  /** Set active. A non-empty map indicates success. */
  private def setActive(tx: Transaction): (Unapplied, Future[S]) = {
    streamStatus.get(tx.stream) match {
      case None =>
        val promise = Promise[S]()
        val active = new Active(Empty, promise, -1)
        if (streamStatus.putIfAbsent(tx.stream, active).isEmpty) {
          (Empty + tx) -> promise.future
        } else setActive(tx)
      case Some(status) if status.isFailure =>
        Empty -> status.promise.future
      case Some(inactive: Inactive) =>
        val promise = inactive.promise
        val active = new Active(Empty, promise, inactive.revision)
        if (streamStatus.replace(tx.stream, inactive, active)) {
          (inactive.unapplied + tx) -> promise.future
        } else setActive(tx)
      case Some(active: Active) =>
        val promise = active.promise
        val newActive = new Active(active.unapplied + tx, promise, active.revision)
        if (streamStatus.replace(tx.stream, active, newActive)) {
          Empty -> promise.future
        } else setActive(tx)
    }
  }

  @tailrec
  private def disable(stream: ID, unapplied: Unapplied, cause: Throwable): Unit = {
    streamStatus.get(stream) match {
      case None => // Already processed, thus inactive
      case Some(status) =>
        val allUnapplied = unapplied ++ status.unapplied
        val inactive = new Inactive(allUnapplied, status.promise, status.revision)
        if (streamStatus.replace(stream, status, inactive)) {
          inactive.promise tryFailure cause
        } else {
          disable(stream, allUnapplied, cause)
        }
    }
  }

  /** Set inactive. An empty list indicates success. */
  @tailrec
  private def setInactive(
      stream: ID, procState: ProcessingState): (Unapplied, Future[S]) =
    streamStatus(stream) match {
      case active: Active =>
        val promise = active.promise
        if (active.unapplied.isEmpty) {
          procState match {
            case Finished(snapshot) =>
              if (streamStatus.remove(stream, active)) {
                promise success snapshot.state
                Empty -> promise.future
              } else setInactive(stream, procState)
            case _ => // Still has unapplied
              if (streamStatus.replace(stream, active, new Inactive(procState.unapplied, promise, procState.revision))) {
                Empty -> promise.future
              } else setInactive(stream, procState)

          }
        } else {
          if (streamStatus.replace(stream, active, new Active(Empty, promise, procState.revision))) {
            active.unapplied ++ procState.unapplied -> promise.future
          } else setInactive(stream, procState)
        }
      case _ => ???
    }

  private[this] val streamStatus = new TrieMap[ID, StreamStatus]

  private[process] case class IncompleteStream(
      id: ID, inProgress: Boolean, expectingRevision: Revision, unappliedRevisions: Option[Range],
      status: Future[S]) {

    // Can return empty range if no revisions are missing
    def missingRevisions: Range =
      expectingRevision until unappliedRevisions.map(_.start).getOrElse(expectingRevision)

    def failure: Option[Throwable] = status.value.flatMap(_.failed.toOption)
  }
  protected def incompleteStreams(): Iterable[IncompleteStream] =
    streamStatus.map {
      case (id, status) =>
        val unappliedRevisions = for {
          first <- status.unapplied.headOption
          last <- status.unapplied.lastOption
        } yield {
          first.revision to last.revision
        }
        IncompleteStream(id, status.isActive, status.revision + 1, unappliedRevisions, status.promise.future)
    }

  protected def onUpdate(id: ID, update: Update): Unit
  protected def onMissingRevisions(id: ID, missing: Range): Unit

  private sealed abstract class ProcessingState {
    def revision: Revision
    def unapplied: Unapplied
    def optSnapshot: Option[Snapshot]
    def snapshotState: Option[S]
    def maxTick(alt: Tick): Tick
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
    def revision = -1
    def maxTick(alt: Tick) = alt
  }
  private case class Intermediate(
    snapshot: Snapshot, unapplied: Unapplied)
  extends ProcessingState { assert(unapplied.nonEmpty)
    def optSnapshot: Option[Snapshot] = Some(snapshot)
    def snapshotState = Some(snapshot.state)
    def revision = snapshot.revision
    def maxTick(alt: Tick) = snapshot.tick max alt
  }
  private case class Finished(
    snapshot: Snapshot)
  extends ProcessingState {
    def unapplied: Unapplied = Empty
    def optSnapshot: Option[Snapshot] = Some(snapshot)
    def snapshotState = Some(snapshot.state)
    def revision = snapshot.revision
    def maxTick(alt: Tick) = snapshot.tick max alt
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
      val expectedRev = procState.revision + 1
      val tx = procState.unapplied.head
      if (tx.revision == expectedRev) {
        callProcess(tx, procState.snapshotState).flatMap { newState =>
          val tick = procState maxTick tx.tick
          val snapshot = Snapshot(newState, tx.revision, tick)
          applyTransactions(ProcessingState(snapshot, procState.unapplied.tail))
        }
      } else if (tx.revision < expectedRev) { // Already processed
        applyTransactions(ProcessingState(procState.optSnapshot, procState.unapplied.tail))
      } else { // tx.revision > expectedRev
        onMissingRevisions(tx.stream, expectedRev until tx.revision)
        Future successful procState
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
        maybeUpdate.foreach { update =>
          try onUpdate(stream, update) catch {
            case NonFatal(notificationFailure) =>
              ec reportFailure notificationFailure
          }
        }
        setInactive(stream, procState) match {
          case (Empty, future) => future
          case (moreUnapplied, _) =>
            upsertUntilInactive(stream, moreUnapplied)
        }
    }

    inactiveFuture.andThen {
      case Failure(cause) => // Unknown cause, but most likely from applying Transaction
        try
          ec reportFailure new RuntimeException(s"Failure when processing stream $stream", cause)
        finally
          disable(stream, unapplied, cause)
    }
  }

  def apply(tx: Transaction): Future[S] =
    setActive(tx) match {
      case (Empty, future) =>
        future
      case (unapplied, _) =>
        upsertUntilInactive(tx.stream, unapplied)(processContext(tx.stream))
    }

}

/**
 * Monotonic replay processor.
 * @see delta.process.EventSourceProcessor
 * @tparam ID Stream identifier
 * @tparam EVT Event type
 * @tparam S State type
 * @tparam U State update type
 */
abstract class MonotonicReplayProcessor[ID, EVT, S >: Null, U]
extends MonotonicProcessor[ID, EVT, S, U]
with AsyncStreamConsumer[Transaction[ID, _ >: EVT], ReplayCompletion[ID]]
with ReplayStatus {

  /** @return Number of active transactions. */
  def activeTransactions: Int = this.activeCount
  /** @return Number of total transactions. */
  def totalTransactions: Long = this.totalCount
  /** @return Number of errors. */
  def numErrors: Int = this.errorCount

  private def instanceName = s"${getClass.getSimpleName}(${processStore.name})"
  override def toString() = s"$instanceName@${hashCode}"

  protected def replayConfig: ReplayProcessConfig
  protected final def completionTimeout = replayConfig.finishProcessingTimeout

  private def incompleteStreams(timeout: Option[TimeoutException]): Try[List[IncompleteStream]] = {

      class IncompleteProcessingTimeout(msg: String, cause: TimeoutException)
      extends TimeoutException(msg) {
        override def getCause(): Throwable = cause
      }

    val incompletes = incompleteStreams().toList
    val inProgressCount = incompletes.count { incomplete =>
      incomplete.inProgress || {
        incomplete.missingRevisions.isEmpty &&
        incomplete.failure.isEmpty
      }
    }
    timeout match {
      case Some(timeout) if inProgressCount > 0 =>
        Failure {
          new IncompleteProcessingTimeout(s"$instanceName has $inProgressCount stream(s) still in progress. Timeout of $completionTimeout is too short! Increase duration to avoid timeout", timeout)
        }
      case _ => Success(incompletes)
    }

  }

  protected def onUpdate(id: ID, update: Update): Unit = () // Don't notify during replay

  /**
   * Missing revision replay is disabled on replay, since
   * the stream will possibly be out of order, and it's
   * considered complete once done.
   */
  protected def onMissingRevisions(id: ID, missing: Range): Unit = ()

  final protected def whenDone(
      timeout: Option[TimeoutException],
      errors: List[Throwable])
      : Future[ReplayCompletion[ID]] = {

    incompleteStreams(timeout) match {
      case Failure(timeout) =>
        Future failed timeout // Hard fail, still in progress, increase timeout
      case Success(incompletes) => // Must be either failed or have missing revisions
        val incompleteStreams =
          incompletes
            .map { incomplete =>
              val missingRevisions =
                incomplete.failure.map(Failure(_)) getOrElse {
                  Success(incomplete.missingRevisions)
                }
              ReplayCompletion.IncompleteStream(incomplete.id, missingRevisions)
            }
        whenDone(ReplayCompletion(totalTransactions, incompleteStreams))
    }

  }

  /**
   * Called when replay processing is completed, successfully
   * or otherwise.
   * This is the time to persist all generated
   * state, and return any information that
   * will be handed over to the live
   * processor, or to fail if the processing state
   * is considered incomplete.
   */
  protected def whenDone(status: ReplayCompletion[ID]): Future[ReplayCompletion[ID]]
}

/**
  * Replay persistence coordination, expecting a
  * `concurrent.Map` to hold replay state.
  */
trait ConcurrentMapReplayPersistence[ID, EVT, S >: Null, U] {
  proc: MonotonicReplayProcessor[ID, EVT, S, U] =>

  type Snapshot = delta.Snapshot[S]
  protected type State = ConcurrentMapStore.State[S]

  /**
    * Persist snapshots from completed replay.
    * @note This is a critical step. If persistence is interrupted,
    * and it's not persisted in tick order (default), it can lead to
    * an invalid tick watermark being reported, which can lead to
    * missed transaction when replaying.
    * To prevent this, clear the store before restart
    * *OR* persist in ascending tick order (default).
    * @param snapshots
    */
  protected def persistReplayState(snapshots: Iterator[(ID, Snapshot)]): Future[Unit]
  protected def onReplayCompletion(): Future[collection.concurrent.Map[ID, State]]

  protected def whenDone(status: ReplayCompletion[ID]): Future[ReplayCompletion[ID]] =
    onReplayCompletion()
      .flatMap { cmap =>

        val snapshots = cmap.iterator.collect {
          case (id, value) if value.updated => id -> value.snapshot
        }

        val persistence =
          if (replayConfig.writeTickOrdered) {
            val snapshotArray = try snapshots.toArray catch {
              case _: OutOfMemoryError =>
                val confSetting = s"${replayConfig.getClass.getSimpleName}(writeTickOrdered = false)"
                throw new OutOfMemoryError(
                  s"Increase available memory, or configure $confSetting")
            }
            cmap.clear()
            Arrays.sort(snapshotArray, MonotonicProcessor.TickComparator[ID, S])
            persistReplayState(snapshotArray.iterator)
          } else {
            persistReplayState(snapshots)
              .andThen { case _ => cmap.clear() }
          }

        persistence.map { _ => status }

      }

}
