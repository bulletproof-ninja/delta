package delta.util

import scala.collection.concurrent.TrieMap

import scuff.MonotonicSequencer
import delta.Transaction

/**
 * Sequenced [[delta.Transaction]] handler.
 * This trait guarantees transactions ordered
 * by revision.
 * NOTICE: This trait expects processing of streams
 * to be serialized, i.e. transactions from the same
 * stream should not be applied concurrently.
 */
trait SequencedProcessor[ID, EVT, CH] extends (Transaction[ID, EVT, CH] => Unit) {

  type TXN = Transaction[ID, EVT, CH]

  /**
   * Callback on duplicate transaction.
   */
  protected def onDuplicate(txn: TXN): Unit = ()
  private def dupeHandler(rev: Int, txn: TXN) = onDuplicate(txn)

  /**
   * Notification that gap is detected. This can be used to initiate
   * replay either immediately or after a certain delay if out-of-order
   * is expected and/or possible.
   */
  protected def onGapDetected(id: ID, expectedRev: Int, actualRev: Int)
  protected def onGapClosed(id: ID)

  /* Called for all buffered transactions, when gap is closed. */
  private def gapClosedConsumer(rev: Int, txn: TXN) = super.apply(txn)

  private[this] val sequencers = new TrieMap[ID, MonotonicSequencer[Int, TXN]]

  private def newSequencer(id: ID, expected: Int) = {
    val gapHandler = new MonotonicSequencer.GapHandler[Int] {
      def gapDetected(expectedRev: Int, actualRev: Int) {
        onGapDetected(id, expectedRev, actualRev)
      }
      def gapClosed() {
        onGapClosed(id)
        sequencers -= id
      }
    }
    val seq = new MonotonicSequencer[Int, TXN](gapClosedConsumer, expected, bufferLimit = 0, gapHandler, dupeHandler)
    sequencers.putIfAbsent(id, seq) match {
      case None => seq
      case Some(otherSeq) => otherSeq
    }
  }

  abstract override def apply(txn: TXN) {
    sequencers.get(txn.stream) match {
      // Out-of-sequence mode
      case Some(sequencer) => sequencer(txn.revision, txn)
      // In-sequence mode
      case None => expectedRevision(txn.stream) match {
        case -1L => super.apply(txn)
        case expected =>
          if (txn.revision == expected) {
            super.apply(txn)
          } else if (txn.revision > expected) {
            val sequencer = newSequencer(txn.stream, expected)
            sequencer(txn.revision, txn)
          } else {
            onDuplicate(txn)
          }
      }
    }

  }

  /**
   * Expected revision for a given stream. Should generally
   * be last seen revision + 1. If unknown stream, there are
   * three choices, depending on strategy:
   *   1. If always interested in complete stream, return `0`. This is the most common behavior.
   *   2. If only interested in new transactions, return `-1`.
   *   3. If not interested, ever, return `Int.MaxValue`. This is uncommon.
   * NOTICE: When using option 3, transactions will be interpreted as duplicates,
   * so make sure duplicates are ignored (default behavior).
   */
  protected def expectedRevision(streamId: ID): Int

}
