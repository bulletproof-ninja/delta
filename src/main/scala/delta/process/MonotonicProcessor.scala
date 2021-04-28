package delta.process

import scala.collection.concurrent.{ TrieMap, Map => CMap }
import scala.util.Failure
import scala.collection.immutable
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

  private def TxRevOrdering[SID] =
    txRevOrdering.asInstanceOf[Ordering[Transaction[SID, Any]]]

  private[this] val tickComparator = new java.util.Comparator[(Any, Snapshot[Any])] {
    def compare(o1: (Any, Snapshot[Any]), o2: (Any, Snapshot[Any])): Int =
      (o1._2.tick - o2._2.tick).toInt
  }

  private[process] def TickComparator[SID, S] = tickComparator.asInstanceOf[java.util.Comparator[(SID, Snapshot[S])]]

}

/**
 * Monotonic processor.
 * @tparam SID Stream identifier type
 * @tparam EVT Event type
 * @tparam S State type
 */
trait MonotonicProcessor[SID, EVT, S >: Null, U]
extends (Transaction[SID, _ >: EVT] => Future[S])
with TransactionProcessor[SID, EVT, S] {

  protected type Snapshot = delta.Snapshot[S]

  protected def processStore: StreamProcessStore[SID, S, U]
  protected def processContext(id: SID): ExecutionContext

  import MonotonicProcessor._
  private[this] val Empty = immutable.TreeSet.empty[Transaction](TxRevOrdering[SID])
  private type Unapplied = immutable.TreeSet[Transaction]

  sealed private abstract class StreamStatus {
    def channel: delta.Channel
    def revision: Revision
    def isActive: Boolean
    def unapplied: Unapplied
    def promise: Promise[S]
    def isFailure: Boolean =
      promise.isCompleted &&
      promise.future.value.get.isFailure
  }
  /** Currently being processed. */
  private final class Active(
      val channel: delta.Channel,
      val unapplied: Unapplied,
      val promise: Promise[S],
      val revision: Revision)
    extends StreamStatus { def isActive = true }
  /** Not currently being processed. */
  private final class Inactive(
    val channel: delta.Channel,
    val unapplied: Unapplied,
    val promise: Promise[S],
    val revision: Revision)
    extends StreamStatus { def isActive = false }

  /** Set active. A non-empty map indicates success. */
  private def setActive(tx: Transaction): (Unapplied, Future[S]) = {
    streamStatus.get(tx.stream) match {
      case None =>
        val promise = Promise[S]()
        val active = new Active(tx.channel, Empty, promise, -1)
        if (streamStatus.putIfAbsent(tx.stream, active).isEmpty) {
          (Empty + tx) -> promise.future
        } else setActive(tx)
      case Some(status) if status.isFailure =>
        Empty -> status.promise.future
      case Some(inactive: Inactive) =>
        val promise = inactive.promise
        val active = new Active(tx.channel, Empty, promise, inactive.revision)
        if (streamStatus.replace(tx.stream, inactive, active)) {
          (inactive.unapplied + tx) -> promise.future
        } else setActive(tx)
      case Some(active: Active) =>
        val promise = active.promise
        val newActive = new Active(tx.channel, active.unapplied + tx, promise, active.revision)
        if (streamStatus.replace(tx.stream, active, newActive)) {
          Empty -> promise.future
        } else setActive(tx)
    }
  }

  @tailrec
  private def inactivate(stream: SID, unapplied: Unapplied, cause: Throwable): Unit = {
    streamStatus.get(stream) match {
      case None => // Already processed, thus inactive
      case Some(status) =>
        val allUnapplied = unapplied ++ status.unapplied
        val inactive = new Inactive(status.channel, allUnapplied, status.promise, status.revision)
        if (streamStatus.replace(stream, status, inactive)) {
          inactive.promise tryFailure cause
        } else {
          inactivate(stream, allUnapplied, cause)
        }
    }
  }

  /** Set inactive. An empty list indicates success. */
  @tailrec
  private def setInactive(
      stream: SID, procState: ProcessingState): (Unapplied, Future[S]) =
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
              val replaced = streamStatus.replace(
                stream,
                active, new Inactive(active.channel, procState.unapplied, promise, procState.revision))
              if (replaced) {
                Empty -> promise.future
              } else setInactive(stream, procState)

          }
        } else {
          val replaced = streamStatus.replace(
            stream,
            active, new Active(active.channel, Empty, promise, procState.revision))
          if (replaced) {
            active.unapplied ++ procState.unapplied -> promise.future
          } else setInactive(stream, procState)
        }
      case _ => ???
    }

  private[this] val streamStatus = new TrieMap[SID, StreamStatus]

  private[process] case class IncompleteStream(
      id: SID,
      channel: delta.Channel,
      inProgress: Boolean,
      expectingRevision: Revision,
      unappliedRevisions: Option[Range],
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
        IncompleteStream(id, status.channel, status.isActive, status.revision + 1, unappliedRevisions, status.promise.future)
    }

  protected def onUpdate(id: SID, update: delta.process.Update[U]): Unit
  protected def onMissingRevisions(id: SID, missing: Range): Unit

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
      stream: SID, unapplied: Unapplied)(
      implicit
      ec: ExecutionContext): Future[S] = {

    val upsertResult: Future[(Option[delta.process.Update[U]], ProcessingState)] =
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
          ec reportFailure new RuntimeException(s"Failure when processing stream `$stream`", cause)
        finally
          inactivate(stream, unapplied, cause)
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
 * @tparam SID Stream identifier
 * @tparam EVT Event type
 * @tparam S State type
 * @tparam U State update type
 */
abstract class MonotonicReplayProcessor[SID, EVT, S >: Null, U](
  protected val replayConfig: ReplayProcessConfig)
extends MonotonicProcessor[SID, EVT, S, U]
with AsyncReduction[Transaction[SID, _ >: EVT], ReplayCompletion[SID]]
with (Transaction[SID, _ >: EVT] => Future[S])
with ReplayStatus {
  final def asyncNext(tx: Transaction): Future[S] = apply(tx)
  override def name = s"${processStore.name}-replay-processor"

  /** @return Number of active transactions. */
  def activeTransactions: Int = this.activeCount
  /** @return Number of total transactions. */
  def totalTransactions: Long = this.totalCount
  /** @return Number of errors. */
  def numErrors: Int = this.errorCount

  override def toString() = s"${getClass.getName}(${processStore.name})@${hashCode}"

  protected final def resultTimeout = replayConfig.completionTimeout

  private def incompleteStreams(
      timeout: Option[TimeoutException])
      : Try[List[IncompleteStream]] = {

      class IncompleteProcessingTimeout(
          msg: String,
          override val getCause: TimeoutException)
      extends TimeoutException(msg)

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
          new IncompleteProcessingTimeout(s"`$name` has $inProgressCount stream(s) still in progress. Timeout of $resultTimeout is too short! Increase duration to avoid timeout", timeout)
        }
      case _ => Success(incompletes)
    }

  }

  // Ignore updates during replay
  protected def onUpdate(id: SID, update: delta.process.Update[U]): Unit = ()

  /**
   * Missing revision replay is disabled on replay, since
   * the stream will possibly be out of order, and it's
   * considered complete once done.
   */
  protected def onMissingRevisions(id: SID, missing: Range): Unit = ()

  final protected def asyncResult(
      timeout: Option[TimeoutException],
      escapedErrors: List[(Transaction, Throwable)])
      : Future[ReplayCompletion[SID]] = {

      import ReplayCompletion._

    incompleteStreams(timeout) match {
      case Failure(timeout) =>
        Future failed timeout // Hard fail, still in progress, resovle by increasing timeout
      case Success(incompletes) => // Must be either failed or have missing revisions
        val brokenStreams = {
          val processingFailures: Map[SID, BrokenStream[SID]] =
            escapedErrors
              .groupBy(_._1.stream)
              .view.mapValues { errors =>
                val (tx, cause) = errors.sortBy(_._1.revision).head
                ProcessingFailure(tx.stream, tx.channel, cause)
              }.toMap
          incompletes.foldLeft(processingFailures) {
            case (brokenStreams, incomplete) =>
              incomplete.failure.map {
                ProcessingFailure(incomplete.id, incomplete.channel, _)
              } orElse incomplete.unappliedRevisions.map {
                MissingRevisions(incomplete.id, incomplete.channel, _)
              } match {
                case Some(broken) => brokenStreams.updated(broken.id, broken)
                case None => brokenStreams // Should not happen, since we're handling incompletes
              }
          }
        }
        asyncResult(ReplayCompletion(totalTransactions, brokenStreams.values.toList))

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
  protected def asyncResult(status: ReplayCompletion[SID]): Future[ReplayCompletion[SID]]

}

trait ReplayPersistence[SID, EVT, S >: Null, U] {
  proc: MonotonicReplayProcessor[SID, EVT, S, U] =>

  protected def persistentStore: StreamProcessStore[SID, S, U]

  /**
    * Persist snapshots from completed replay.
    * @note This is a critical step. If persistence is interrupted,
    * and it's not persisted in tick order (default), it can lead to
    * an invalid tick watermark being reported, which can lead to
    * missed transaction when replaying.
    * To prevent this, clear the store before restart
    * *OR* persist in ascending tick order (default).
    * @param snapshots The snapshots to persist
    * @param isTickOrdered `true` if snapshots are ordered by tick
    * @param batchSize Size of batches to write
    */
  protected def persistReplayState(
      snapshots: IterableOnce[(SID, Snapshot)],
      isTickOrdered: Boolean,
      batchSize: Int)
      : Future[WriteErrors] =
    if (snapshots.isEmpty) NoErrorsFuture
    else if (isTickOrdered) persistSequential(batchSize, snapshots)
    else persistParallel(batchSize, snapshots)

  type WriteErrors = Map[SID, Throwable]
  private val NoErrors = Map.empty[SID, Throwable]
  private val NoErrorsFuture = Future successful NoErrors

  protected implicit def completionContext: ExecutionContext

  private def persistSequential(
      batchSize: Int,
      snapshots: IterableOnce[(SID, Snapshot)])
      : Future[WriteErrors] =
    if (batchSize > 1) {

        def persistSequentialAsync(
          batches: List[IterableOnce[(SID, Snapshot)]],
          allWriteErrors: WriteErrors)
          : Future[WriteErrors] =
          batches match {
            case nextBatch :: remaining =>
              (persistentStore writeBatch nextBatch)
                .flatMap { writeErrors =>
                  persistSequentialAsync(remaining, allWriteErrors ++ writeErrors)
                }
            case Nil =>
              Future successful allWriteErrors
          }

      persistSequentialAsync(snapshots.iterator.grouped(batchSize).toList, NoErrors)

    } else {

      val futureWrites = snapshots.iterator.map {
        case idSnapshot @ (id, _) => id -> (persistentStore write idSnapshot)
      }
      Future.sequenceTry(futureWrites)(_._2, _._1)
        .map { _
          .collect {
              case (Failure(error), key) => key -> error
            }
          .foldLeft(NoErrors) { _ + _ }
        }

    }

  private def persistParallel(
      batchSize: Int,
      snapshots: IterableOnce[(SID, Snapshot)])
      : Future[WriteErrors] = {

    assert(snapshots.nonEmpty)

    val futureWriteErrors: IterableOnce[Future[WriteErrors]] =
      if (batchSize > 1) {
        snapshots.iterator.grouped(batchSize)
          .map { batch =>
            persistentStore writeBatch batch
          }
      } else {
        snapshots.map {
          case (id, snapshot) =>
            persistentStore.write(id, snapshot)
              .map { _ => NoErrors }
              .recover {
                case NonFatal(cause) => Map(id -> cause)
              }
        }
      }

    Future.reduceLeft(futureWriteErrors.to(Seq)) { _ ++ _ }

  }


}

/**
  * Replay persistence coordination, expecting a
  * `concurrent.Map` to hold replay state.
  */
trait ConcurrentMapReplayPersistence[SID, EVT, S >: Null, U]
extends ReplayPersistence[SID, EVT, S, U] {
  proc: MonotonicReplayProcessor[SID, EVT, S, U] =>

  protected type ReplayState = delta.process.ReplayState[S]

  protected def onReplayCompletion(): Future[CMap[SID, ReplayState]]

  protected def asyncResult(
      status: ReplayCompletion[SID])
      : Future[ReplayCompletion[SID]] =
    onReplayCompletion()
      .flatMap { snapshotMap =>

        val persistence = try writeSnapshots(snapshotMap) catch {
          case _: OutOfMemoryError =>
            System.gc()
            val confSetting = s"${replayConfig.getClass.getSimpleName}(writeTickOrdered = false)"
            throw new OutOfMemoryError(
              s"Too many snapshot to sort in-memory. Either increase available memory, or configure $confSetting")
        }

        persistence.map { _ => status }

      }

  private def writeSnapshots(snapshotMap: CMap[SID, ReplayState]): Future[WriteErrors] = {
    val snapshots = snapshotMap.iterator.collect {
      case (id, state) if state.updated => id -> state.snapshot
    }
    if (replayConfig.writeTickOrdered) {
      val snapshotArray = snapshots.toArray
      snapshotMap.clear()
      Arrays.sort(snapshotArray, MonotonicProcessor.TickComparator[SID, S])
      persistReplayState(snapshotArray.iterator, true, replayConfig.writeBatchSize)
    } else {
      persistReplayState(snapshots, false, replayConfig.writeBatchSize)
        .andThen { case _ => snapshotMap.clear() }
    }
  }

}
