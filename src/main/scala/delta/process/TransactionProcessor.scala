package delta.process

import scala.concurrent.Future
import scala.util.control.NonFatal

/**
 * @tparam ID Stream identifier
 * @tparam EVT Top event type
 * @tparam S state type
 */
trait TransactionProcessor[SID, EVT, S >: Null] {

  def name: String

  protected type Transaction = delta.Transaction[SID, _ >: EVT]

  /**
   *  Transaction processing.
   *  @param tx Transaction to process
   *  @param currState Current state, if exists.
   *  @return New state
   */
  protected def process(tx: Transaction, currState: Option[S]): Future[S]

  @inline
  private[process] final def callProcess(tx: Transaction, currState: Option[S]): Future[S] =
    try process(tx, currState) catch {
      case NonFatal(cause) =>
        Future failed new IllegalStateException(
s"""Failed processing of transaction ${tx.stream} (revision ${tx.revision})
----
State: $currState
Transaction: $tx
----""", cause)
    }

  /** Convenience wrapping of state into `Future`. */
  implicit protected final def toFuture(state: S): Future[S] = Future successful state

}
