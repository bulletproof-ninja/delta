package ulysses

import scala.collection.{ Seq, Map }
import scala.concurrent.ExecutionContext
import scuff.{ Subscription, Feed }
import scala.concurrent.Future
import scala.util.Success
import scala.util.control.NonFatal
import scuff.concurrent.StreamCallback
import scuff.Subscription

trait Publishing[ID, EVT, CH]
    extends EventStore[ID, EVT, CH] {

  /** Execution context for calling `publish(TXN)`. */
  protected def publishCtx: ExecutionContext
  /** This call will be executed in the `publishCtx`. */
  protected def publish(txn: TXN): Unit
  private def publish(txn: Future[TXN]): Unit = {
    txn.foreach { txn =>
      try publish(txn) catch {
        case NonFatal(e) => publishCtx reportFailure e
      }
    }(publishCtx)
  }
  abstract override def commit(
    channel: CH, stream: ID, revision: Int, tick: Long,
    events: Seq[EVT], metadata: Map[String, String]): Future[TXN] = {
    val txn = super.commit(channel, stream, revision, tick, events, metadata)
    publish(txn)
    txn
  }
  def subscribe(
    filter: StreamFilter[ID, EVT, CH] = StreamFilter.Everything())(
      callback: StreamCallback[TXN]): Subscription
}
