package ulysses.util

import ulysses._
import scuff._
import java.util.Date
import concurrent._
import scala.util._
import java.util.concurrent.TimeUnit
import scala.annotation.implicitNotFound
import language.implicitConversions
import scala.concurrent._
import collection.immutable.Seq
import collection.concurrent.TrieMap

/**
 * Non-persistent implementation, probably only useful for testing.
 */
@implicitNotFound("Cannot find an implicit ExecutionContext, either import scala.concurrent.ExecutionContext.Implicits.global or use a custom one")
class TransientEventStore[ID, EVT, CAT](execCtx: ExecutionContext)
    extends EventStore[ID, EVT, CAT] {

  @inline
  implicit private def ec = execCtx

  protected def publishCtx = execCtx

  private[this] val txnMap = new TrieMap[ID, Vector[Transaction]]

  def lastTimestamp: Future[Long] = Future {
    if (txnMap.isEmpty) {
      Long.MinValue
    } else {
      txnMap.values.flatten.map(_.clock).max
    }
  }

  private def findCurrentRevision(id: ID): Option[Int] = txnMap.get(id).map(_.last.revision)

  def currRevision(stream: ID): Future[Option[Int]] = Future(findCurrentRevision(stream))

  def record(clock: Long, category: CAT, streamId: ID, revision: Int, events: Seq[EVT], metadata: Map[String, String]): Future[Transaction] = Future {
    val transactions = txnMap.getOrElse(streamId, Vector[Transaction]())
    val expectedRev = transactions.size
    if (revision == expectedRev) {
      val txn = Transaction(clock, category, streamId, revision, metadata, events)
      if (revision == 0) {
        txnMap.putIfAbsent(streamId, transactions :+ txn).foreach { existing =>
          throw new DuplicateRevisionException(existing(revision))
        }
      } else {
        val success = txnMap.replace(streamId, transactions, transactions :+ txn)
        if (!success) {
          throw new DuplicateRevisionException(txnMap(streamId)(revision))
        }
      }
      txn
    } else if (expectedRev > revision) {
      throw new DuplicateRevisionException(transactions(revision))
    } else {
      throw new IllegalStateException(s"$streamId revision $revision too large, expected $expectedRev")
    }
  }

  private def withCallback(callback: StreamCallback[Transaction])(thunk: => Unit): Unit =
    Future(thunk).onComplete {
      case Success(_) => callback.onCompleted()
      case Failure(t) => callback.onError(t)
    }

  def replayStream(stream: ID)(callback: StreamCallback[Transaction]): Unit = withCallback(callback) {
    val txns = txnMap.getOrElse(stream, Vector.empty)
    txns.foreach(callback.onNext)
  }
  def replayStreamFrom(stream: ID, fromRevision: Int)(callback: StreamCallback[Transaction]): Unit =
    replayStreamRange(stream, fromRevision to Int.MaxValue)(callback)
  def replayStreamTo(stream: ID, toRevision: Int)(callback: StreamCallback[Transaction]): Unit =
    replayStreamRange(stream, 0 to toRevision)(callback)
  def replayStreamRange(stream: ID, revisionRange: collection.immutable.Range)(callback: StreamCallback[Transaction]): Unit = withCallback(callback) {
    val txns = txnMap.getOrElse(stream, Vector.empty)
    val sliced = revisionRange.last match {
      case Int.MaxValue => txns.drop(revisionRange.head)
      case last => txns.slice(revisionRange.head, last+1)
    }
    sliced.foreach(callback.onNext)
  }
  def replay(categories: CAT*)(callback: StreamCallback[Transaction]): Unit = withCallback(callback) {
    txnMap.valuesIterator.filter(matchCategories(categories.toSet)).flatten.toSeq.sortBy(_.clock).foreach(callback.onNext)
  }
  def replayFrom(fromTimestamp: Long, categories: CAT*)(callback: StreamCallback[Transaction]): Unit = withCallback(callback) {
    txnMap.valuesIterator.filter(matchCategories(categories.toSet)).map(removeOutdated(fromTimestamp)).flatten.toSeq.sortBy(_.clock).foreach(callback.onNext)
  }

  private def matchCategories(categories: Set[CAT])(vec: Vector[Transaction]): Boolean = categories.isEmpty || categories.contains(vec(0).category)
  private def removeOutdated(fromTimestamp: Long)(vector: Vector[Transaction]): Vector[Transaction] = vector.dropWhile(_.clock < fromTimestamp)
}
