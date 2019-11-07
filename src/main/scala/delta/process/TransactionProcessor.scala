package delta.process

import delta.Transaction
import scala.concurrent.Future
import scala.util.control.NonFatal

/**
 * @tparam ID Stream identifier
 * @tparam EVT Top event type
 * @tparam S state type
 */
trait TransactionProcessor[ID, EVT, S >: Null] {

  protected type TXN = Transaction[ID, _ >: EVT]

  /**
   *  Transaction processing.
   *  @param txn Transaction to process
   *  @param currState Current state, if exists.
   *  @return New state
   */
  protected def process(txn: TXN, currState: Option[S]): Future[S]

  @inline
  private[process] final def callProcess(txn: TXN, currState: Option[S]): Future[S] =
    try process(txn, currState) catch {
      case NonFatal(cause) =>
        Future failed new IllegalStateException(
s"""Failed processing of transaction ${txn.stream}:${txn.revision}
Preprocess state: $currState
Transaction: $txn
""", cause)
    }

  /** Convenience wrapping of state into `Future`. */
  implicit protected final def toFuture(state: S): Future[S] = Future successful state

}
