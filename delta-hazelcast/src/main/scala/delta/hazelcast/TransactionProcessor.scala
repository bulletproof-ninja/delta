package delta.hazelcast

import java.util.Map.Entry

import scala.annotation.tailrec
import scala.collection.immutable.TreeMap

import com.hazelcast.map.AbstractEntryProcessor

import delta.Transaction
import delta.ddd.Fold
import delta.cqrs.ReadModel

case class ReadModelState[D, EVT](
  model: ReadModel[D],
  dataUpdated: Boolean = false,
  unapplied: TreeMap[Int, Transaction[_, EVT, _]] = TreeMap.empty[Int, Transaction[_, EVT, _]])

sealed trait ModelUpdateResult
case object IgnoredDuplicate extends ModelUpdateResult
case class MissingRevisions(range: Range) extends ModelUpdateResult
case class Updated[D](model: ReadModel[D]) extends ModelUpdateResult

final class ReadModelUpdater[K, D >: Null, EVT](
  val txn: Transaction[K, EVT, _],
  val modelUpdater: Fold[D, EVT])
    extends AbstractEntryProcessor[K, ReadModelState[D, EVT]](true) {

  type S = ReadModelState[D, EVT]

  private def apply(events: List[EVT], dataOrNull: D): D = events match {
    case Nil => dataOrNull.ensuring(_ != null)
    case evt :: tail => dataOrNull match {
      case null => apply(tail, modelUpdater.init(evt))
      case data => apply(tail, modelUpdater.next(data, evt))
    }
  }

  def process(entry: Entry[K, S]): Object = processTransaction(entry, this.txn)

  @tailrec
  private def processTransaction(entry: Entry[K, S], txn: Transaction[_, EVT, _]): ModelUpdateResult = {
    entry.getValue match {

      case null => // First transaction seen
        if (txn.revision == 0) { // First transaction, as expected
          val model = new ReadModel(apply(txn.events, null), txn.revision, txn.tick)
          entry setValue new S(model, true)
          Updated(model)
        } else { // Not first, so missing some
          entry setValue new S(null, false, TreeMap(txn.revision -> txn))
          MissingRevisions(0 until txn.revision)
        }

      case ReadModelState(null, _, unapplied) => // Un-applied transactions exists, no model yet
        if (txn.revision == 0) { // This transaction is first, so apply
          val model = new ReadModel(apply(txn.events, null), txn.revision, txn.tick)
          entry setValue new S(model, true, unapplied.tail)
          processTransaction(entry, unapplied.head._2)
        } else { // Still not first transaction
          val state = new S(null, false, unapplied.updated(txn.revision, txn))
          entry setValue state
          MissingRevisions(0 until state.unapplied.head._1)
        }

      case ReadModelState(model, _, unapplied) =>
        val expectedRev = model.revision + 1
        if (txn.revision == expectedRev) { // Expected revision, apply
          val updModel = new ReadModel(apply(txn.events, model.data), txn.revision, txn.tick)
          unapplied.headOption match {
            case None =>
              val dataUpdated = model.data != updModel.data
              entry setValue new S(updModel, dataUpdated)
              Updated(updModel)
            case Some((_, unappliedTxn)) =>
              entry setValue new S(updModel, false, unapplied.tail)
              processTransaction(entry, unappliedTxn)
          }
        } else if (txn.revision > expectedRev) { // Future revision, missing some
          val state = new S(model, false, unapplied.updated(txn.revision, txn))
          entry setValue state
          MissingRevisions(expectedRev until state.unapplied.head._1)
        } else {
          IgnoredDuplicate
        }

    }
  }

}
