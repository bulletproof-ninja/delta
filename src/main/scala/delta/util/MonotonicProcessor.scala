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

abstract class MonotonicProcessor[ID, EVT, S >: Null](
    protected val processStore: StreamProcessStore[ID, S])(
    implicit protected val evtTag: ClassTag[EVT])
  extends TransactionProcessor[ID, EVT, S]
  with (Transaction[ID, _ >: EVT] => Future[Unit]) {

  protected type TXN = Transaction[ID, _ >: EVT]
  type Snapshot = delta.Snapshot[S]
  type Update = processStore.Update

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
  protected def unfinishedStreams: collection.Set[ID] = streamStatus.keySet

  protected def onUpdate(id: ID, update: Update): Unit
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
        Future successful snapshot -> Nil
    }

  }

  private def upsertUntilInactive(stream: ID, unapplied: List[TXN])(
      implicit ec: ExecutionContext): Future[Unit] = {
    val upsertResult = processStore.upsert(stream) { existingSnapshot =>
      applyTransactions(unapplied.sorted(MonotonicProcessor.RevOrdering), existingSnapshot)
    }
    val inactiveFuture = upsertResult.flatMap {
      case (update, stillUnapplied) =>
        update.foreach(onUpdate(stream, _))
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
  * The default batch processor.
  * @see delta.util.EventSourceProcessor
  */
abstract class MonotonicBatchProcessor[ID, EVT: ClassTag, S >: Null, BR](
    protected val completionTimeout: FiniteDuration,
    processStore: StreamProcessStore[ID, S])
  extends MonotonicProcessor[ID, EVT, S](processStore)
  with AsyncStreamConsumer[Transaction[ID, _ >: EVT], BR] {

  override def onDone(): Future[BR] = {
    super.onDone().map { res =>
      require(unfinishedStreams.isEmpty, s"Stream processing must be finished by now!")
      res
    }(scuff.concurrent.Threads.PiggyBack)
  }

  protected def onUpdate(id: ID, update: Update): Unit = ()
  protected def onMissingRevisions(id: ID, missing: Range): Unit = ()

  /**
    *  Called when batch processing is done.
    *  This is the time to persist all generated
    *  state, and return any information that
    *  will be handed over to the real-time
    *  processor.
    */
  protected def whenDone(): Future[BR]
}

trait ConcurrentMapBatchProcessing[ID, EVT, S >: Null, BR] {
  proc: MonotonicBatchProcessor[ID, EVT, S, BR] =>

  protected def whenDoneContext: ExecutionContext
  protected def onBatchStreamCompletion(): Future[collection.concurrent.Map[ID, Snapshot]]
  protected def persist(snapshots: collection.concurrent.Map[ID, Snapshot]): Future[BR]
  protected def isUnfinishedStreamsFatal = true
  protected def whenDone(): Future[BR] = {
      implicit def ec = whenDoneContext
    onBatchStreamCompletion()
      .flatMap { cmap =>
        if (isUnfinishedStreamsFatal && unfinishedStreams.nonEmpty) {
          val ids = unfinishedStreams.mkString(compat.Platform.EOL, ", ", "")
          throw new IllegalStateException(s"Incomplete stream processing for ids:$ids")
        }
        persist(cmap --= unfinishedStreams).andThen {
          case _ => cmap.clear()
        }
      }
  }
}
