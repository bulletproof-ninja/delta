package delta.util

import delta._
import scala.concurrent._
import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag
import scuff.concurrent.AsyncStreamConsumer
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure

private object MonotonicProcessor {
  private type TXN = Transaction[_, _]
  val RevOrdering = new Ordering[TXN] {
    def compare(x: TXN, y: TXN): Int = {
      assert(x.stream == y.stream)
      x.revision - y.revision
    }
  }
}

trait MonotonicProcessor[ID, EVT, S >: Null]
  extends TransactionProcessor[ID, EVT, S]
  with (Transaction[ID, _ >: EVT] => Future[Unit]) {

  type Snapshot = delta.Snapshot[S]
  type SnapshotUpdate = delta.util.SnapshotUpdate[S]

  protected def processStore: StreamProcessStore[ID, S]
  protected def processingContext(id: ID): ExecutionContext

  sealed private abstract class StreamStatus
  /** Currently being processed. */
  private final class Active(val unapplied: List[TXN], val p: Promise[Unit]) extends StreamStatus
  /** Not currently being processed. */
  private final class Inactive(val unapplied: List[TXN], val p: Promise[Unit]) extends StreamStatus

  /** Set active. A non-empty list indicates success. */
  private def setActive(txn: TXN): (List[TXN], Future[Unit]) = {
    streamStatus.get(txn.stream) match {
      case None =>
        val promise = Promise[Unit]
        if (streamStatus.putIfAbsent(txn.stream, new Active(Nil, promise)).isEmpty) List(txn) -> promise.future
        else setActive(txn)
      case Some(inactive: Inactive) =>
        val promise = inactive.p
        if (streamStatus.replace(txn.stream, inactive, new Active(Nil, promise))) (txn :: inactive.unapplied) -> promise.future
        else setActive(txn)
      case Some(active: Active) =>
        val promise = active.p
        if (streamStatus.replace(txn.stream, active, new Active(txn :: active.unapplied, promise))) Nil -> promise.future
        else setActive(txn)
    }
  }
  private def forceInactive(stream: ID, unapplied: List[TXN]): Unit = {
    assert(unapplied.nonEmpty)
    streamStatus(stream) match {
      case active: Active =>
        val promise = active.p
        val allUnapplied = (unapplied ++ active.unapplied).distinct
        if (!streamStatus.replace(stream, active, new Inactive(allUnapplied, promise))) {
          forceInactive(stream, allUnapplied)
        }
      case inactive: Inactive =>
        val promise = inactive.p
        val allUnapplied = (unapplied ++ inactive.unapplied).distinct
        if (!streamStatus.replace(stream, inactive, new Inactive(allUnapplied, promise))) {
          forceInactive(stream, allUnapplied)
        }
    }
  }
  /** Set inactive. An empty list indicates success. */
  private def setInactive(stream: ID, stillUnapplied: List[TXN]): List[TXN] = {
    streamStatus(stream) match {
      case active: Active =>
        val promise = active.p
        if (active.unapplied.isEmpty) {
          if (stillUnapplied.isEmpty) {
            if (streamStatus.remove(stream, active)) {
              promise.success(())
              Nil
            } else setInactive(stream, stillUnapplied)
          } else {
            if (streamStatus.replace(stream, active, new Inactive(stillUnapplied, promise))) Nil
            else setInactive(stream, stillUnapplied)
          }
        } else {
          if (streamStatus.replace(stream, active, new Active(Nil, promise))) active.unapplied ++ stillUnapplied
          else setInactive(stream, stillUnapplied)
        }
      case _ => ???
    }
  }

  private[this] val streamStatus = new TrieMap[ID, StreamStatus]
  protected def incompleteStreams: collection.Set[ID] = streamStatus.keySet

  protected def onSnapshotUpdate(id: ID, update: SnapshotUpdate): Unit
  protected def onMissingRevisions(id: ID, missing: Range): Unit

  private def applyTransactions(
      txns: List[TXN], snapshot: Option[Snapshot])(
      implicit ec: ExecutionContext): Future[(Option[Snapshot], List[TXN])] = {
    txns match {
      case txn :: remainingTxns =>
        val expectedRev = snapshot.map(_.revision + 1) getOrElse 0
        if (txn.revision == expectedRev) {
          processAsync(txn, snapshot.map(_.content)).flatMap { updated =>
            val tick = snapshot.map(_.tick max txn.tick) getOrElse txn.tick
            applyTransactions(remainingTxns, Some(Snapshot(updated, txn.revision, tick)))
          }
        } else if (txn.revision < expectedRev) { // Already processed
          applyTransactions(remainingTxns, snapshot)
        } else { // txn.revision > expectedRev
          onMissingRevisions(txn.stream, expectedRev until txn.revision)
          Future successful (snapshot -> txns)
        }
      case Nil =>
        Future successful (snapshot -> Nil)
    }

  }

  private def upsertUntilInactive(stream: ID, unapplied: List[TXN])(
      implicit ec: ExecutionContext): Future[Unit] = {
    val upsertResult = processStore.upsert(stream) { existingSnapshot =>
      applyTransactions(unapplied.sorted(MonotonicProcessor.RevOrdering), existingSnapshot)
    }
    val inactiveFuture = upsertResult.flatMap {
      case (update, stillUnapplied) =>
        update.foreach(upd => onSnapshotUpdate(stream, upd))
        setInactive(stream, stillUnapplied) match {
          case Nil => Future successful (())
          case moreUnapplied => upsertUntilInactive(stream, moreUnapplied)
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
      case (Nil, future) => future
      case (unapplied, _) =>
        upsertUntilInactive(txn.stream, unapplied)(processingContext(txn.stream))
    }

  }
}

/**
 * Monotonic replay processor.
 * @see delta.util.EventSourceProcessor
 * @tparam ID Stream identifier
 * @tparam EVT Event type
 * @tparam S State type
 * @tparam BR Batch result. Typically just `Unit`
 */
abstract class MonotonicReplayProcessor[ID, EVT, S >: Null, BR](
    protected val completionTimeout: FiniteDuration,
    protected val processStore: StreamProcessStore[ID, S])(
    implicit protected val evtTag: ClassTag[EVT])
  extends MonotonicProcessor[ID, EVT, S]
  with AsyncStreamConsumer[Transaction[ID, _ >: EVT], BR] {

  override def onDone(): Future[BR] = {
    super.onDone().map { res =>
      require(incompleteStreams.isEmpty, s"Stream processing must be finished by now!")
      res
    }(scuff.concurrent.Threads.PiggyBack)
  }

  protected def onSnapshotUpdate(id: ID, update: SnapshotUpdate): Unit = ()
  protected def onMissingRevisions(id: ID, missing: Range): Unit = ()

  /**
   *  Called when replay processing is done.
   *  This is the time to persist all generated
   *  state, and return any information that
   *  will be handed over to the live
   *  processor.
   */
  protected def whenDone(): Future[BR]
}

trait ConcurrentMapReplayProcessing[ID, EVT, S >: Null, BR] {
  proc: MonotonicReplayProcessor[ID, EVT, S, BR] =>

  protected def whenDoneContext: ExecutionContext
  protected def onReplayCompletion(): Future[collection.concurrent.Map[ID, Snapshot]]
  protected def persist(snapshots: collection.concurrent.Map[ID, Snapshot]): Future[BR]
  protected def isUnfinishedStreamsFatal = true
  protected def whenDone(): Future[BR] = {
      implicit def ec = whenDoneContext
    onReplayCompletion()
      .flatMap { cmap =>
        if (isUnfinishedStreamsFatal && incompleteStreams.nonEmpty) {
          val ids = incompleteStreams.mkString(compat.Platform.EOL, ", ", "")
          throw new IllegalStateException(s"Incomplete stream processing for ids:$ids")
        }
        persist(cmap --= incompleteStreams).andThen {
          case _ => cmap.clear()
        }
      }
  }
}
