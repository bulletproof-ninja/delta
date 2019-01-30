package delta.util

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

  protected def process(txn: TXN, currState: Option[S]): S

  protected def processAsync(txn: TXN, currState: Option[S]): Future[S] =
    try Future successful process(txn, currState) catch {
      case NonFatal(th) => Future failed th
    }

}
