package ulysses

import scala.concurrent.ExecutionContext
import scuff.{ Subscription, Feed }
import scala.concurrent.Future
import scala.util.Success
import scala.util.control.NonFatal
import scuff.concurrent.StreamCallback
import scuff.Subscription

/**
  * Enable pub/sub of transactions.
  */
trait Publishing[ID, EVT, CH]
    extends EventStore[ID, EVT, CH] {

  private def publish(txn: Future[TXN]): Unit = {
    txn.foreach { txn =>
      try publish(txn) catch {
        case NonFatal(e) => publishCtx reportFailure e
      }
    }(publishCtx)
  }
  abstract override def commit(
    channel: CH, stream: ID, revision: Int, tick: Long,
    events: List[EVT], metadata: Map[String, String]): Future[TXN] = {
    val txn = super.commit(channel, stream, revision, tick, events, metadata)
    publish(txn)
    txn
  }

  /** The execution context to publish on. */
  protected def publishCtx: ExecutionContext

  /**
   * Publish transaction. This will happen on
   * the `publishCtx` execution context.
   */
  protected def publish(txn: TXN): Unit

}
