package delta.util

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.TreeSet
import scala.concurrent.Future
import scala.concurrent.duration.{ DurationInt, FiniteDuration }
import scala.util.control.NonFatal

import delta.{ Snapshot, SnapshotStore, Transaction }
import scuff.concurrent._

trait MonotonicProcessor[ID, EVT, CH, S]
    extends (Transaction[ID, EVT, CH] => Unit) {

  type TXN = Transaction[ID, EVT, CH]

  /**
    * The execution context to ensure same-thread
    * processing of streams.
    * NOTE: Because strict ordering is guaranteed,
    * execution will block on the provided threads,
    * thus the number of threads should not necessarily
    * be limited to the number of cores.
    */
  protected def exeCtx: PartitionedExecutionContext
  protected def snapshots: SnapshotStore[ID, S]
  protected def onSnapshotUpdated(id: ID, snapshot: Snapshot[S]): Unit
  protected def process(txn: TXN, currState: Option[S]): S

  protected def snapshotStoreTimeout: FiniteDuration = _defaultTimeout
  private[this] val _defaultTimeout = 10.seconds

  private def updateOnPartitionThread(
    key: ID, thunk: Option[Snapshot[S]] => Option[Snapshot[S]]): Option[Snapshot[S]] = {

    val oldSnapshot = snapshots.read(key).await(snapshotStoreTimeout, exeCtx.reportFailure)
    val newSnapshot = thunk(oldSnapshot)
    if (oldSnapshot ne newSnapshot) newSnapshot.foreach { newSnapshot =>
      if (oldSnapshot.forall(_.content != newSnapshot.content)) {
        snapshots.write(key, newSnapshot).await(snapshotStoreTimeout, exeCtx.reportFailure)
        onSnapshotUpdated(key, newSnapshot)
      } else if (oldSnapshot.get.revision != newSnapshot.revision || oldSnapshot.get.tick != newSnapshot.tick) {
        snapshots.refresh(key, newSnapshot.revision, newSnapshot.tick).await(snapshotStoreTimeout, exeCtx.reportFailure)
        onSnapshotUpdated(key, newSnapshot)
      }
    }
    newSnapshot
  }

  /** Thread-safe update of snapshot. */
  protected def updateSnapshot(key: ID)(
    thunk: Option[Snapshot[S]] => Option[Snapshot[S]]): Future[Option[Snapshot[S]]] =
    exeCtx.submit(key.##)(updateOnPartitionThread(key, thunk))

  private[this] val unappliedTxns = new TrieMap[ID, TreeSet[TXN]]

  private def RevOrdering = new Ordering[TXN] {
    def compare(x: TXN, y: TXN): Int = x.revision - y.revision
  }

  @annotation.tailrec
  private def processUnapplied(
    currState: Option[Snapshot[S]],
    unapplied: TreeSet[TXN]): (Option[Snapshot[S]], TreeSet[TXN]) = {

    val expectedRevision = currState.map(_.revision + 1) getOrElse 0

    unapplied.headOption match {
      case Some(txn) if txn.revision == expectedRevision =>
        val currTick = currState.map(_.tick) getOrElse Long.MinValue
        val updated = process(txn, currState.map(_.content))
        processUnapplied(Some(Snapshot(updated, txn.revision, currTick max txn.tick)), unapplied.tail)
      case Some(txn) if txn.revision < expectedRevision =>
        processUnapplied(currState, unapplied.tail)
      case _ =>
        currState -> unapplied
    }
  }

  @annotation.tailrec
  private def saveUnapplied(key: ID, unapplied: TreeSet[TXN]): Unit = {
    unappliedTxns.putIfAbsent(key, unapplied) match {
      case None => // Ok!
      case Some(other) =>
        val combined = unapplied ++ other
        if (!unappliedTxns.replace(key, other, combined)) {
          saveUnapplied(key, combined)
        }
    }
  }
  @annotation.tailrec
  private def saveUnappliedIfEmptyElseReturnAllUnapplied(key: ID, unapplied: TreeSet[TXN]): Option[TreeSet[TXN]] = {
    unappliedTxns.putIfAbsent(key, unapplied) match {
      case None => None
      case Some(existing) =>
        if (unappliedTxns.remove(key, existing)) {
          Some(existing ++ unapplied)
        } else {
          saveUnappliedIfEmptyElseReturnAllUnapplied(key, unapplied)
        }
    }
  }

  def apply(txn: TXN): Unit = exeCtx.submit(txn.stream.##) {
    val toBeApplied = unappliedTxns.remove(txn.stream) match {
      case None => TreeSet(txn)(RevOrdering)
      case Some(unapplied) => unapplied + txn
    }
      @annotation.tailrec
      def updateThunk(toBeApplied: TreeSet[TXN])(snapshot: Option[Snapshot[S]]): Option[Snapshot[S]] = {
        val (updatedSnapshot, unapplied) = processUnapplied(snapshot, toBeApplied)
        if (unapplied.isEmpty) updatedSnapshot
        else {
          saveUnappliedIfEmptyElseReturnAllUnapplied(txn.stream, unapplied) match {
            case None => updatedSnapshot
            case Some(combinedUnapplied) =>
              updateThunk(combinedUnapplied)(updatedSnapshot)
          }
        }
      }
    try updateOnPartitionThread(txn.stream, updateThunk(toBeApplied)) catch {
      case NonFatal(th) =>
        exeCtx.reportFailure(th)
        saveUnapplied(txn.stream, toBeApplied)
    }
  }
}
