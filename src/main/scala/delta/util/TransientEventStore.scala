package delta.util

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

import scuff.StreamConsumer
import delta.{ EventCodec, EventStore }

/**
  * Non-persistent implementation, probably only useful for testing.
  */
abstract class TransientEventStore[ID, EVT, SF](
  execCtx: ExecutionContext)(implicit codec: EventCodec[EVT, SF])
    extends EventStore[ID, EVT] {

  private def Txn(id: ID, rev: Int, ch: String, tick: Long, metadata: Map[String, String], events: List[EVT]): Txn = {
    val eventsSF = events.map { evt =>
      (codec.name(evt), codec.version(evt), codec.encode(evt))
    }
    new Txn(id, rev, ch, tick, metadata, eventsSF)
  }
  private class Txn(
      id: ID,
      val rev: Int,
      ch: String,
      val tick: Long,
      metadata: Map[String, String],
      eventsSF: List[(String, Byte, SF)]) {
    def toTransaction: TXN = {
      val events = eventsSF.map {
        case (name, version, data) =>
          codec.decode(name, version, data)
      }
      Transaction(tick, ch, id, rev, metadata, events)
    }
  }

  implicit private def ec = execCtx

  private[this] val txnMap = new TrieMap[ID, Vector[Txn]]

  def maxTick(): Future[Option[Long]] = {
    val ticks = Future(txnMap.values.iterator.flatten.map(_.tick))
    ticks.map { ticks =>
      if (ticks.isEmpty) None
      else Some(ticks.max)
    }
  }
  private def findCurrentRevision(id: ID): Option[Int] = txnMap.get(id).map(_.last.rev)

  def currRevision(stream: ID): Future[Option[Int]] = Future(findCurrentRevision(stream))

  def commit(
    channel: String, stream: ID, revision: Int, tick: Long,
    events: List[EVT], metadata: Map[String, String]): Future[TXN] = Future {
    val transactions = txnMap.getOrElse(stream, Vector[Txn]())
    val expectedRev = transactions.size
    if (revision == expectedRev) {
      val txn = Txn(stream, revision, channel, tick, metadata.toMap, events)
      if (revision == 0) {
        txnMap.putIfAbsent(stream, transactions :+ txn).foreach { existing =>
          throw new DuplicateRevisionException(existing(0).toTransaction)
        }
      } else {
        val success = txnMap.replace(stream, transactions, transactions :+ txn)
        if (!success) {
          throw new DuplicateRevisionException(txnMap(stream)(revision).toTransaction)
        }
      }
      txn.toTransaction
    } else if (expectedRev > revision) {
      throw new DuplicateRevisionException(transactions(revision).toTransaction)
    } else {
      throw new IllegalStateException(s"$stream revision $revision too large, expected $expectedRev")
    }
  }

  private def withCallback[U](callback: StreamConsumer[TXN, U])(thunk: => Unit): Unit = Future {
    Try(thunk) match {
      case Success(_) => callback.onDone()
      case Failure(th) => callback.onError(th)
    }
  }

  def replayStream[E >: EVT, U](stream: ID)(callback: StreamReplayConsumer[E, U]): Unit = withCallback(callback) {
    val txns = txnMap.getOrElse(stream, Vector.empty)
    txns.map(_.toTransaction).foreach(callback.onNext)
  }
  def replayStreamFrom[E >: EVT, U](stream: ID, fromRevision: Int)(callback: StreamReplayConsumer[E, U]): Unit =
    replayStreamRange(stream, fromRevision to Int.MaxValue)(callback)
  def replayStreamRange[E >: EVT, U](stream: ID, revisionRange: collection.immutable.Range)(callback: StreamReplayConsumer[E, U]): Unit = withCallback(callback) {
    val txns = txnMap.getOrElse(stream, Vector.empty)
    val sliced = revisionRange.last match {
      case Int.MaxValue => txns.drop(revisionRange.head)
      case last => txns.slice(revisionRange.head, last + 1)
    }
    sliced.map(_.toTransaction).foreach(callback.onNext)
  }
  def query[U](selector: Selector)(callback: StreamConsumer[TXN, U]): Unit = withCallback(callback) {
    txnMap.valuesIterator.flatten
      .map(_.toTransaction)
      .filter(selector.include)
      .foreach(callback.onNext)
  }
  def querySince[U](sinceTick: Long, selector: Selector)(callback: StreamConsumer[TXN, U]): Unit = withCallback(callback) {
    txnMap.valuesIterator.flatten
      .filter(_.tick >= sinceTick)
      .map(_.toTransaction)
      .filter(selector.include)
      .foreach(callback.onNext)
  }
}
