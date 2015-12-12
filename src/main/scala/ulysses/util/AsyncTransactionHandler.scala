package ulysses.util

import ulysses._

/**
 * Asynchronous [[ulysses.EventSource#Transaction]] handler.
 * This trait supports [[scuff.concurrent.HashPartitionExecutionContext]]
 * to ensure serialized stream execution.
 */
trait AsyncTransactionHandler[ID, EVT, CAT] extends (EventSource[ID, EVT, CAT]#Transaction => Unit) {

  /**
   * Execution context.
   * Supports `HashPartitionExecutionContext`.
   */
  protected def asyncTransactionCtx: concurrent.ExecutionContext

  private type TXN = EventSource[ID, EVT, CAT]#Transaction

  @inline private def callSuper(txn: TXN) = try {
    super.apply(txn)
  } catch {
    case t: Throwable => asyncTransactionCtx.reportFailure(ProcessingException(t, txn))
  }

  abstract override def apply(txn: TXN) {
    asyncTransactionCtx execute new Runnable {
      override def hashCode = txn.streamId.hashCode
      def run = callSuper(txn)
    }
  }

}
