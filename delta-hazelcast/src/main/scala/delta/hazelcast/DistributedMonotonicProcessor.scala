package delta.hazelcast

import java.util.Map.Entry

import scala.annotation.tailrec
import scala.collection.immutable.TreeMap
import scala.concurrent._, duration._
import scala.reflect.{ ClassTag, classTag }

import delta._
import delta.process.{ AsyncCodec, Update }

import com.hazelcast.core._
import com.hazelcast.map.AbstractEntryProcessor

import scuff.Codec
import scuff.concurrent._

object DistributedMonotonicProcessor {

  /**
    * Process transaction, ensuring proper sequencing.
    */
  def apply[K, EVT: ClassTag, S >: Null: ClassTag](
      imap: IMap[K, _ <: EntryState[S, EVT]], projector: Projector[S, EVT])(
      tx: Transaction[K, _ >: EVT]): Future[EntryUpdateResult] =
    this.apply(Codec.noop[S], imap, projector)(tx)

  /**
    * Process transaction, ensuring proper sequencing.
    */
  def apply[K, EVT: ClassTag, W >: Null: ClassTag, S](
      stateCodec: Codec[W, S],
      imap: IMap[K, _ <: EntryState[S, EVT]], projector: Projector[W, EVT])(
      tx: Transaction[K, _ >: EVT]): Future[EntryUpdateResult] = {

    val verifiedTx: Transaction[K, EVT] = {
      tx.events.collect { case evt: EVT => evt } match {
        case Nil => sys.error(s"${tx.channel} transaction ${tx.stream}(rev:${tx.revision}) events does not conform to ${classTag[EVT].runtimeClass.getName}")
        case events => tx.copy(events = events)
      }
    }
    val processor = new DistributedMonotonicProcessor[K, EVT, W, S](
      AsyncCodec(stateCodec), Offloadable.NO_OFFLOADING, verifiedTx, projector)
    val callback = CallbackPromise[EntryUpdateResult]
    imap.submitToKey(tx.stream, processor, callback)
    callback.future
  }

  /**
    * Process transaction, ensuring proper sequencing.
    */
  def apply[K, EVT: ClassTag, W >: Null: ClassTag, S](
      stateCodec: AsyncCodec[W, S],
      stateCodecExecutor: IExecutorService,
      imap: IMap[K, _ <: EntryState[S, EVT]],
      projector: Projector[W, EVT])(
      tx: Transaction[K, _ >: EVT]): Future[EntryUpdateResult] = {

    val verifiedTx: Transaction[K, EVT] = {
      tx.events.collect { case evt: EVT => evt } match {
        case Nil => sys.error(s"${tx.channel} transaction ${tx.stream}(rev:${tx.revision}) events does not conform to ${classTag[EVT].runtimeClass.getName}")
        case events => tx.copy(events = events)
      }
    }
    val processor = new DistributedMonotonicProcessor[K, EVT, W, S](
      stateCodec, stateCodecExecutor.getName, verifiedTx, projector)
    val callback = CallbackPromise[EntryUpdateResult]
    imap.submitToKey(tx.stream, processor, callback)
    callback.future
  }

}

/**
 *  Distributed monotonic [[delta.Transaction]] entry processor, ensuring
 *  monotonic stream revision ordering.
 */
final class DistributedMonotonicProcessor[K, EVT, W >: Null, S] private[hazelcast] (
  val stateCodec: AsyncCodec[W, S], val getExecutorName: String,
  val tx: delta.Transaction[K, EVT],
  val projector: Projector[W, EVT])
extends AbstractEntryProcessor[K, EntryState[S, EVT]](/* applyOnBackup */ true)
with Offloadable {

  type EntryState = delta.hazelcast.EntryState[S, EVT]
  type Transaction = delta.Transaction[_, EVT]

  def process(entry: Entry[K, EntryState]): Object = processTransaction(entry, this.tx)

  private def processTransaction(entry: Entry[K, EntryState], tx: Transaction): EntryUpdateResult = {

      def updateEntry(entryState: EntryState): EntryUpdateResult = {

        entry setValue entryState

        if (entryState.unapplied.isEmpty) { // No unapplied, so must have snapshot
          val snapshot = entryState.snapshot
          val updContent = if (entryState.contentUpdated) Some(snapshot.state) else None
          Updated(Update(updContent, snapshot.revision, snapshot.tick))
        } else { // Have unapplied, so revisions still missing
          val firstMissingRev = entryState.snapshot match {
            case null => 0
            case snapshot => snapshot.revision + 1
          }
          val range = firstMissingRev until entryState.unapplied.head._1
          assert(range.nonEmpty)
          MissingRevisions(range)
        }
      }

    entry.getValue match {

      case null => // First transaction seen
        val currUnapplied = TreeMap(tx.revision -> tx)
        if (tx.revision == 0) { // First transaction, as expected
          val entryState = processTransactions(currUnapplied)
          updateEntry(entryState)
        } else { // Not first, so missing some
          val entryState = new EntryState(null, contentUpdated = false, currUnapplied)
          updateEntry(entryState)
        }

      case EntryState(null, _, unapplied) =>
        assert(unapplied.nonEmpty) // No snapshot, so unapplied transactions must exist
        val currUnapplied = unapplied.updated(tx.revision, tx)
        if (tx.revision == 0) { // This transaction is first, so apply
          val entryState = processTransactions(currUnapplied)
          updateEntry(entryState)
        } else { // Still not first transaction
          val entryState = new EntryState(null, contentUpdated = false, currUnapplied)
          updateEntry(entryState)
        }

      case EntryState(snapshot, _, unapplied) =>
        if (snapshot.revision >= tx.revision) IgnoredDuplicate
        else {
          val currUnapplied = unapplied.updated(tx.revision, tx)
          val entryState = processTransactions(currUnapplied, snapshot)
          updateEntry(entryState)
        }
    }
  }

  private def processTransactions(
      unapplied: TreeMap[Int, delta.Transaction[_, EVT]], snapshot: Snapshot[S]): EntryState = {
    val state = stateCodec decode snapshot.state
    processTransactions(
      unapplied, Some(snapshot.revision -> snapshot.state),
      Some(state), snapshot.revision, snapshot.tick)
  }

  @tailrec
  private def processTransactions(
      unapplied: TreeMap[Int, delta.Transaction[_, EVT]], origState: Option[(Int, S)] = None,
      currState: Option[W] = None, revision: Revision = -1, tick: Tick = Long.MinValue): EntryState = {

    unapplied.headOption match {

      case Some((_, tx)) if tx.revision == revision + 1 =>
        val newState = projector(tx, currState)
        processTransactions(unapplied.tail, origState, Some(newState), tx.revision, tx.tick)

      case _ =>
        val (snapshotOrNull, contentUpdated) = currState match {
          case Some(currState) =>
            origState match {
              case Some((origRev, origState)) if origRev == revision =>
                Snapshot(origState, revision, tick) -> false
              case _ =>
                val encoding = stateCodec.encode(currState)(Threads.PiggyBack)
                val newState = encoding await Duration.Zero // Duration irrelevant when piggybacking
                val snapshot = Snapshot(newState, revision, tick)
                val contentUpdated = !origState.exists(snapshot.stateEquals)
                snapshot -> contentUpdated
            }
          case None => (null: Snapshot[S]) -> false
        }
        EntryState(snapshotOrNull, contentUpdated, unapplied)
    }

  }

}
