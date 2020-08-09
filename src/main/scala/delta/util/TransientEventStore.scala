package delta.util

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

import scuff.StreamConsumer
import delta._

/**
  * Non-persistent implementation, probably only useful for testing.
  */
abstract class TransientEventStore[ID, EVT, SF](
  execCtx: ExecutionContext, evtFmt: EventFormat[EVT, SF])(
  initTicker: TransientEventStore[ID, EVT, SF] => Ticker)
    extends EventStore[ID, EVT] {

  lazy val ticker = initTicker(this)

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

  def commit(
    channel: Channel, stream: ID, revision: Revision, tick: Tick,
    events: List[EVT], metadata: Map[String, String]): Future[Transaction] = Future {
    val transactions = txMap.getOrElse(stream, Vector[Tx]())
    val expectedRev = transactions.size
    if (revision == expectedRev) {
      val tx = Tx(stream, revision, channel, tick, metadata.toMap, events)
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

  private def withCallback[U](callback: StreamConsumer[Transaction, U])(thunk: => Unit): Unit = Future {
    Try(thunk) match {
      case Success(_) => callback.onDone()
      case Failure(th) => callback.onError(th)
    }
  }

  def replayStream[R](stream: ID)(callback: StreamConsumer[Transaction, R]): Unit = withCallback(callback) {
    val txs = txMap.getOrElse(stream, Vector.empty)
    txs.map(_.toTransaction).foreach(callback.onNext)
  }
  def replayStreamFrom[R](stream: ID, fromRevision: Revision)(callback: StreamConsumer[Transaction, R]): Unit =
    replayStreamRange(stream, fromRevision to Int.MaxValue)(callback)
  def replayStreamRange[R](stream: ID, revisionRange: collection.immutable.Range)(callback: StreamConsumer[Transaction, R]): Unit = withCallback(callback) {
    val txs = txMap.getOrElse(stream, Vector.empty)
    val sliced = revisionRange.last match {
      case Int.MaxValue => txs.drop(revisionRange.head)
      case last => txs.slice(revisionRange.head, last + 1)
    }
    sliced.map(_.toTransaction).foreach(callback.onNext)
  }
  def query[U](
      selector: Selector)(
      callback: StreamConsumer[Transaction, U]): Unit = withCallback(callback) {
    txMap.valuesIterator.flatten
      .map(_.toTransaction)
      .filter(selector.include)
      .foreach(callback.onNext)
  }
  def querySince[U](
      sinceTick: Tick, selector: Selector)(
      callback: StreamConsumer[Transaction, U]): Unit = withCallback(callback) {
    txMap.valuesIterator.flatten
      .filter(_.tick >= sinceTick)
      .map(_.toTransaction)
      .filter(selector.include)
      .foreach(callback.onNext)
  }
}
