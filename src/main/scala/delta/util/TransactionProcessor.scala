package delta.util

import delta.Transaction
import scala.concurrent.Future
import scala.util.control.NonFatal

trait TransactionProcessor[ID, EVT, S >: Null] {
  protected def process(txn: Transaction[ID, _ >: EVT], currState: Option[S]): S
  protected def processAsync(txn: Transaction[ID, _ >: EVT], currState: Option[S]): Future[S] =
    try Future successful process(txn, currState) catch {
      case NonFatal(th) => Future failed th
    }

}
