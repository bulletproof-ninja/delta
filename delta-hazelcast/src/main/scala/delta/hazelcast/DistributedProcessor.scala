package delta.hazelcast

import java.util.Map.Entry

import scala.annotation.tailrec
import scala.collection.immutable.TreeMap

import com.hazelcast.map.AbstractEntryProcessor

import delta.{ EventReducer, Snapshot, Transaction }
import com.hazelcast.core.IMap
import scala.concurrent.Future
import com.hazelcast.core.ExecutionCallback
import scala.concurrent.Promise
import scala.reflect.{ ClassTag, classTag }

case class EntryState[D, EVT](
  snapshot: Snapshot[D],
  contentUpdated: Boolean = false,
  unapplied: TreeMap[Int, Transaction[_, EVT]] = TreeMap.empty[Int, Transaction[_, EVT]])

sealed abstract class EntryUpdateResult
case object IgnoredDuplicate extends EntryUpdateResult
case class MissingRevisions(range: Range) extends EntryUpdateResult
case class Updated[D](snapshot: Snapshot[D]) extends EntryUpdateResult

object DistributedProcessor {
  /**
    * Process transaction, ensuring proper sequencing.
    */
  def apply[K, D >: Null, EVT: ClassTag](imap: IMap[K, EntryState[D, EVT]], reducer: EventReducer[D, EVT])(
    txn: Transaction[K, _ >: EVT]): Future[EntryUpdateResult] = {
    val verifiedTxn: Transaction[K, EVT] = {
      txn.events.collect { case evt: EVT => evt } match {
        case Nil => sys.error(s"${txn.channel} transaction ${txn.stream}(rev:${txn.revision}) events does not conform to ${classTag[EVT].runtimeClass.getName}")
        case events => txn.copy(events = events)
      }
    }
    val processor = new DistributedProcessor[K, D, EVT](verifiedTxn, reducer)
    val promise = Promise[EntryUpdateResult]()
    val callback = new ExecutionCallback[EntryUpdateResult] {
      def onResponse(response: EntryUpdateResult) = promise success response
      def onFailure(th: Throwable) = promise failure th
    }
    imap.submitToKey(txn.stream, processor, callback)
    promise.future
  }
}

/**
 *  Distributed monotonic [[delta.Transaction]] entry processor, ensuring
 *  monotonic stream revision ordering.
 */
final class DistributedProcessor[K, D >: Null, EVT] private[hazelcast] (
  val txn: Transaction[K, EVT],
  val reducer: EventReducer[D, EVT])(implicit val evtTag: ClassTag[EVT])
    extends AbstractEntryProcessor[K, EntryState[D, EVT]](true) {

  type S = EntryState[D, EVT]
  type TXN = Transaction[_, EVT]

  def process(entry: Entry[K, S]): Object = processTransaction(entry, this.txn)

  @tailrec
  private def processTransaction(entry: Entry[K, S], txn: TXN): EntryUpdateResult = {
    entry.getValue match {

      case null => // First transaction seen
        if (txn.revision == 0) { // First transaction, as expected
          val snapshot = new Snapshot(EventReducer.process(reducer)(None, txn.events), txn.revision, txn.tick)
          entry setValue new S(snapshot, true)
          Updated(snapshot)
        } else { // Not first, so missing some
          entry setValue new S(null, false, TreeMap(txn.revision -> txn))
          MissingRevisions(0 until txn.revision)
        }

      case EntryState(null, _, unapplied) => // Un-applied transactions exists, no snapshot yet
        if (txn.revision == 0) { // This transaction is first, so apply
          val snapshot = new Snapshot(EventReducer.process(reducer)(None, txn.events), txn.revision, txn.tick)
          entry setValue new S(snapshot, true, unapplied.tail)
          processTransaction(entry, unapplied.head._2)
        } else { // Still not first transaction
          val state = new S(null, false, unapplied.updated(txn.revision, txn))
          entry setValue state
          MissingRevisions(0 until state.unapplied.head._1)
        }

      case EntryState(snapshot, _, unapplied) =>
        val expectedRev = snapshot.revision + 1
        if (txn.revision == expectedRev) { // Expected revision, apply
          val updSnapshot = new Snapshot(EventReducer.process(reducer)(Some(snapshot.content), txn.events), txn.revision, txn.tick)
          unapplied.headOption match {
            case None =>
              val dataUpdated = !(snapshot contentEquals updSnapshot)
              entry setValue new S(updSnapshot, dataUpdated)
              Updated(updSnapshot)
            case Some((_, unappliedTxn)) =>
              entry setValue new S(updSnapshot, false, unapplied.tail)
              processTransaction(entry, unappliedTxn)
          }
        } else if (txn.revision > expectedRev) { // Future revision, missing some
          val state = new S(snapshot, false, unapplied.updated(txn.revision, txn))
          entry setValue state
          MissingRevisions(expectedRev until state.unapplied.head._1)
        } else {
          IgnoredDuplicate
        }

    }
  }

}
