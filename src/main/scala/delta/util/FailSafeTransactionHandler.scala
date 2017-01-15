package delta.util

import delta._
import scala.util.control.NonFatal

/**
 * Fail safe [[delta.Transaction]] handler.
 * This handler will exclude any streams that fails, to ensure
 * continuous processing of all other streams.
 */
trait FailSafeTransactionHandler[ID, EVT, CH] extends (Transaction[ID, EVT, CH]=> Unit) {

  /** Determine if stream is failed. */
  protected def isFailed(stream: ID): Boolean
  /** Mark stream as failed. Re-throw exception if propagation desirable. */
  protected def markFailed(stream: ID, ch: CH, t: Throwable)

  abstract override def apply(txn: Transaction[ID, EVT, CH]) {
    if (!isFailed(txn.stream)) try {
      super.apply(txn)
    } catch {
      case NonFatal(th) => markFailed(txn.stream, txn.channel, th)
    }
  }

}
