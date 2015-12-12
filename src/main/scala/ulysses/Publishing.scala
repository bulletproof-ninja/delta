package ulysses

import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext
import scuff.Faucet
import scala.concurrent.Future
import scala.util.Success
import scala.util.control.NonFatal

trait Publishing[ID, EVT, CAT] extends EventStore[ID, EVT, CAT] with Faucet {

  type Filter = CAT
  type Consumer = (Transaction => Unit)

  /** Execution context for publishing. */
  protected def publishCtx: ExecutionContext
  /** This call will be executed in the `publishCtx`. */
  protected def publish(txn: Transaction): Unit
  private def publish(txn: Future[Transaction]): Unit = {
    txn.foreach { txn =>
      try publish(txn) catch {
        case NonFatal(e) => publishCtx reportFailure e
      }
    }(publishCtx)
  }
  abstract override def createStream(streamId: ID, category: CAT, clock: Long, events: Seq[EVT], metadata: Map[String, String]): Future[Transaction] = {
    val txn = super.createStream(streamId, category, clock, events, metadata)
    publish(txn)
    txn
  }
  abstract override def appendStream(
    streamId: ID, revision: Int, clock: Long, events: Seq[EVT], metadata: Map[String, String]): Future[Transaction] = {
    val txn = super.appendStream(streamId, revision, clock, events, metadata)
    publish(txn)
    txn
  }

}
