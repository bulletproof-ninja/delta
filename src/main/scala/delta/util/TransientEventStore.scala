package delta.util

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ ExecutionContext, Future }

import scuff.Reduction
import delta._

/**
  * Non-persistent implementation, probably only useful for testing.
  */
abstract class TransientEventStore[ID, EVT, SF](
  execCtx: ExecutionContext, evtFmt: EventFormat[EVT, SF])
extends EventStore[ID, EVT] {

  protected def publishCtx = execCtx

  def subscribeGlobal[U](
      selector: StreamsSelector)(
      callback: Transaction => U) =
    subscribeLocal(selector)(callback)

  private def Tx(id: ID, rev: Revision, channel: Channel, tick: Tick, metadata: Map[String, String], events: List[EVT]): Tx = {
    val eventsSF = events.map { evt =>
      val EventFormat.EventSig(name, version) = evtFmt.signature(evt)
      (name, version, evtFmt encode evt)
    }
    new Tx(id, rev, channel, tick, metadata, eventsSF)
  }
  private class Tx(
      id: ID,
      val rev: Revision,
      channel: Channel,
      val tick: Tick,
      metadata: Map[String, String],
      eventsSF: List[(String, Byte, SF)]) {
    def toTransaction: Transaction = {
      val events = eventsSF.map {
        case (name, version, data) =>
          evtFmt.decode(name, version, data, channel, metadata)
      }
      Transaction(tick, channel, id, rev, metadata, events)
    }
  }

  implicit private def ec = execCtx

  private[this] val txMap = new TrieMap[ID, Vector[Tx]]

  def maxTick: Future[Option[Long]] = {
    val ticks = Future(txMap.values.iterator.flatten.map(_.tick))
    ticks.map { ticks =>
      if (ticks.isEmpty) None
      else Some(ticks.max)
    }
  }
  private def findCurrentRevision(id: ID): Option[Int] = txMap.get(id).map(_.last.rev)

  def currRevision(stream: ID): Future[Option[Int]] = Future(findCurrentRevision(stream))

  protected def commit(
    tick: Tick,
    channel: Channel,
    stream: ID,
    revision: Revision,
    metadata: Map[String, String],
    events: List[EVT])
    : Future[Transaction] = Future {
    val transactions = txMap.getOrElse(stream, Vector[Tx]())
    val expectedRev = transactions.size
    if (revision == expectedRev) {
      val tx = Tx(stream, revision, channel, tick, metadata, events)
      if (revision == 0) {
        txMap.putIfAbsent(stream, transactions :+ tx).foreach { existing =>
          throw new DuplicateRevisionException(existing(0).toTransaction)
        }
      } else {
        val success = txMap.replace(stream, transactions, transactions :+ tx)
        if (!success) {
          throw new DuplicateRevisionException(txMap(stream)(revision).toTransaction)
        }
      }
      tx.toTransaction
    } else if (expectedRev > revision) {
      throw new DuplicateRevisionException(transactions(revision).toTransaction)
    } else {
      throw new IllegalStateException(s"$stream revision $revision too large, expected $expectedRev")
    }
  }

  private def withReduction[U](reduction: Reduction[Transaction, Future[U]])(thunk: => Unit): Future[U] =
    Future {
      thunk
      reduction.result()
    }.flatten

  def replayStream[R](
      stream: ID)(
      reduction: Reduction[Transaction, Future[R]])
      : Future[R] =
    withReduction(reduction) {
      val txs = txMap.getOrElse(stream, Vector.empty)
      txs.map(_.toTransaction).foreach(reduction.next)
    }

  def replayStreamFrom[R](
      stream: ID,
      fromRevision: Revision)(
      reduction: Reduction[Transaction, Future[R]])
      : Future[R] =
    replayStreamRange(stream, fromRevision to Int.MaxValue)(reduction)

  def replayStreamRange[R](
      stream: ID, revisionRange: collection.immutable.Range)(
      reduction: Reduction[Transaction, Future[R]])
      : Future[R] =
    withReduction(reduction) {
      val txs = txMap.getOrElse(stream, Vector.empty)
      val sliced = revisionRange.last match {
        case Int.MaxValue => txs.drop(revisionRange.head)
        case last => txs.slice(revisionRange.head, last + 1)
      }
      sliced.map(_.toTransaction).foreach(reduction.next)
    }

  def query[U](
      selector: Selector)(
      reduction: Reduction[Transaction, Future[U]])
      : Future[U] =
    withReduction(reduction) {
      txMap.valuesIterator.flatten
        .map(_.toTransaction)
        .filter(selector.include)
        .foreach(reduction.next)
    }

  def querySince[U](
      sinceTick: Tick, selector: Selector)(
      reduction: Reduction[Transaction, Future[U]])
      : Future[U] =
    withReduction(reduction) {
      txMap.valuesIterator.flatten
        .filter(_.tick >= sinceTick)
        .map(_.toTransaction)
        .filter(selector.include)
        .foreach(reduction.next)
    }
}
