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
   *  Synchronous processing. If processing is asynchronous,
   *  instead override `processAsync` and make this a no-op.
   *  @param txn Transaction to process
   *  @param currState Current state, if exists.
   */
  protected def process(txn: TXN, currState: Option[S]): S

  /**
   *  Asynchronous processing. The default implementation
   *  simply delegates to `process`.
   *  @param txn Transaction to process
   *  @param currState Current state, if exists.
   */
  protected def processAsync(txn: TXN, currState: Option[S]): Future[S] =
    try Future successful process(txn, currState) catch {
      case NonFatal(th) => Future failed th
    }

}
