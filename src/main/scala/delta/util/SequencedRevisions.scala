package delta.util

import delta.Transaction
import scuff.MonotonicSequencer
import scala.collection.concurrent.TrieMap
import scuff.concurrent.StreamCallback

/**
  * This trait will ensure fully ordered sequencing
  * of transactions, according to the stream's
  * revision number. Duplicates, to the extent they
  * can ever occur, will be ignored.
  * NOTE: This should not be used when getting incomplete
  * streams, e.g. when querying/subscribing to specific
  * events, such as `EventSelector`. 
  */
trait SequencedRevisions[ID, EVT, CH]
    extends StreamCallback[Transaction[ID, EVT, CH]] {
  type TXN = Transaction[ID, EVT, CH]
  protected def expectedRevision(id: ID): Int
  private[this] val sequencers = new TrieMap[ID, MonotonicSequencer[Int, TXN]]
  private def newSequencer(expectedRev: Int) = new MonotonicSequencer[Int, TXN](consume, expectedRev, dupeConsumer = (_: Int, _: TXN) => ())
  private def consume(rev: Int, txn: TXN): Unit = super.onNext(txn)
  abstract override def onNext(txn: TXN): Unit = {
    val sequencer = sequencers.getOrElseUpdate(txn.stream, newSequencer(expectedRevision(txn.stream)))
    sequencer(txn.revision, txn)
  }
  abstract override def onCompleted() = {
    sequencers.clear()
    super.onCompleted()
  }
}
