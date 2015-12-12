package ulysses.util

import ulysses._

/**
 * Fail safe [[ulysses.EventSource#Transaction]] handler.
 * This handler will exclude any streams that fails, to ensure
 * continuous processing of all other streams.
 */
trait FailSafeTransactionHandler[ID, EVT, CAT] extends (EventSource[ID, EVT, CAT]#Transaction => Unit) {

  type Transaction = EventSource[ID, EVT, CAT]#Transaction

  /** Determine if stream is failed. */
  protected def isFailed(stream: ID): Boolean
  /** Mark stream as failed. Re-throw exception to fail further up. */
  protected def markFailed(stream: ID, cat: CAT, t: Throwable)

  abstract override def apply(txn: Transaction) {
    if (!isFailed(txn.streamId)) try {
      super.apply(txn)
    } catch {
      case t: Throwable => markFailed(txn.streamId, txn.category, t)
    }
  }

}
