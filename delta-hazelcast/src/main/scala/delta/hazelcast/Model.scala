package delta.hazelcast

import java.util.Map.Entry

import scala.annotation.tailrec
import scala.collection.immutable.TreeMap

import com.hazelcast.map.AbstractEntryProcessor

import delta.Transaction
import delta.ddd.Fold

case class Model[D](
  data: D,
  revision: Int,
  tick: Long)

case class State[D, EVT](
  model: Model[D] = null,
  unapplied: TreeMap[Int, Transaction[_, EVT, _]] = TreeMap.empty)

sealed trait Result
case object IgnoredDuplicate extends Result
case class MissingRevisions(range: Range) extends Result
case class Updated[D](model: Model[D]) extends Result

final class ModelUpdater[K, D >: Null, EVT](
  txn: Transaction[K, EVT, _],
  fold: Fold[D, EVT])
    extends AbstractEntryProcessor[K, State[D, EVT]](true) {

  type S = State[D, EVT]

  private def apply(events: List[EVT], dataOrNull: D): D = events match {
    case Nil => dataOrNull.ensuring(_ != null)
    case evt :: tail => dataOrNull match {
      case null => apply(tail, fold.init(evt))
      case data => apply(tail, fold.next(data, evt))
    }
  }

  def process(entry: Entry[K, S]): Object = processTransaction(entry, this.txn)

  @tailrec
  private def processTransaction(entry: Entry[K, S], txn: Transaction[_, EVT, _]): Result = {
    entry.getValue match {

      case null => // First transaction seen
        if (txn.revision == 0) { // First transaction, as expected
          val model = new Model(apply(txn.events, null), txn.revision, txn.tick)
          entry setValue new S(model)
          Updated(model)
        } else { // Not first, so missing some
          entry setValue new S(unapplied = TreeMap(txn.revision -> txn))
          MissingRevisions(0 until txn.revision)
        }

      case State(null, unapplied) => // Un-applied transactions exists, no model yet
        if (txn.revision == 0) { // This transaction is first, so apply
          val model = new Model(apply(txn.events, null), txn.revision, txn.tick)
          entry setValue new S(model, unapplied.tail)
          processTransaction(entry, unapplied.head._2)
        } else { // Still not first transaction
          val state = new S(unapplied = unapplied.updated(txn.revision, txn))
          entry setValue state
          MissingRevisions(0 until state.unapplied.head._1)
        }

      case State(model, unapplied) =>
        val expectedRev = model.revision + 1
        if (txn.revision == expectedRev) { // Expected revision, apply
          val updModel = new Model(apply(txn.events, model.data), txn.revision, txn.tick)
          unapplied.headOption match {
            case None =>
              entry setValue new S(updModel)
              Updated(updModel)
            case Some((_, unappliedTxn)) =>
              entry setValue new S(updModel, unapplied.tail)
              processTransaction(entry, unappliedTxn)
          }
        } else if (txn.revision > expectedRev) { // Future revision, missing some
          val state = new S(model, unapplied.updated(txn.revision, txn))
          entry setValue state
          MissingRevisions(expectedRev until state.unapplied.head._1)
        } else {
          IgnoredDuplicate
        }

    }
  }

}
