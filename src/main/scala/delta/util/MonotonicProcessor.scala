package delta.util

import delta._
import scala.concurrent._
import scala.collection.concurrent.TrieMap
import scala.util._, control.NonFatal
import scuff.concurrent.Threads

private object MonotonicProcessor {
  private type TXN = Transaction[_, _, _]
  val RevOrdering = new Ordering[TXN] {
    def compare(x: TXN, y: TXN): Int = {
      assert(x.stream == y.stream)
      x.revision - y.revision
    }
  }
}

trait MonotonicProcessor[ID, EVT, S]
  extends (Transaction[ID, _ >: EVT, _ >: Any] => Future[Unit]) {

  type TXN = Transaction[ID, _ >: EVT, _ >: Any]
  type Snapshot = delta.Snapshot[S]
  type Update = processStore.Update

  protected val processStore: StreamProcessStore[ID, S]
  protected def executionContext(id: ID): ExecutionContext

  sealed private abstract class StreamStatus
  /** Currently being processed. */
  private final case class Active(unapplied: List[TXN]) extends StreamStatus
  /** Not currently being processed. */
  private final case class Inactive(unapplied: List[TXN]) extends StreamStatus

  private final val ActiveNil = Active(Nil)
  /** Set active. A non-empty list indicates success. */
  private def setActive(txn: TXN): List[TXN] = {
    streamStatus.get(txn.stream) match {
      case None =>
        if (streamStatus.putIfAbsent(txn.stream, ActiveNil).isEmpty) List(txn)
        else setActive(txn)
      case Some(inactive @ Inactive(unapplied)) =>
        if (streamStatus.replace(txn.stream, inactive, ActiveNil)) txn :: unapplied
        else setActive(txn)
      case Some(active @ Active(unapplied)) =>
        if (streamStatus.replace(txn.stream, active, Active(txn :: unapplied))) Nil
        else setActive(txn)
    }
  }
  private def forceInactive(stream: ID, unapplied: List[TXN]): Unit = {
    assert(unapplied.nonEmpty)
    streamStatus(stream) match {
      case status @ Active(newUnapplied) =>
        val allUnapplied = (unapplied ++ newUnapplied).distinct
        if (!streamStatus.replace(stream, status, Inactive(allUnapplied))) {
          forceInactive(stream, allUnapplied)
        }
      case status @ Inactive(newUnapplied) =>
        val allUnapplied = (unapplied ++ newUnapplied).distinct
        if (!streamStatus.replace(stream, status, Inactive(allUnapplied))) {
          forceInactive(stream, allUnapplied)
        }
    }
  }
  /** Set inactive. An empty list indicates success. */
  private def setInactive(stream: ID, stillUnapplied: List[TXN]): List[TXN] = {
    val active @ Active(newUnapplied) = streamStatus(stream)
    if (newUnapplied.isEmpty) {
      if (stillUnapplied.isEmpty) {
        if (streamStatus.remove(stream, active)) Nil
        else setInactive(stream, stillUnapplied)
      } else {
        if (streamStatus.replace(stream, active, Inactive(stillUnapplied))) Nil
        else setInactive(stream, stillUnapplied)
      }
    } else {
      if (streamStatus.replace(stream, active, ActiveNil)) newUnapplied ++ stillUnapplied
      else setInactive(stream, stillUnapplied)
    }
  }

  private[this] val streamStatus = new TrieMap[ID, StreamStatus]

  protected def onUpdate(id: ID, update: Update): Unit
  protected def onMissingRevisions(id: ID, missing: Range): Unit

  protected def process(txn: TXN, currState: Option[S]): S
  protected def processAsync(txn: TXN, currState: Option[S]): Future[S] =
    try Future successful process(txn, currState) catch {
      case NonFatal(th) => Future failed th
    }

  private def applyTransactions(txns: List[TXN], snapshot: Option[Snapshot]): Future[(Option[Snapshot], List[TXN])] = {
    txns match {
      case txn :: remainingTxns =>
        val expectedRev = snapshot.map(_.revision + 1) getOrElse 0
        if (txn.revision == expectedRev) {
          processAsync(txn, snapshot.map(_.content)).flatMap { updated =>
            val tick = snapshot.map(_.tick max txn.tick) getOrElse txn.tick
            applyTransactions(remainingTxns, Some(Snapshot(updated, txn.revision, tick)))
          }(Threads.PiggyBack)
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
      case Failure(_) =>
        forceInactive(stream, unapplied)
    }
  }

  def apply(txn: TXN): Future[Unit] = {
    setActive(txn) match {
      case Nil => Future successful Unit
      case unapplied =>
        upsertUntilInactive(txn.stream, unapplied)(executionContext(txn.stream))
    }

  }
}
