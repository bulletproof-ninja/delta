package delta

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal

import scuff.Codec

/**
  * Enable pub/sub of transactions.
  */
trait Publishing[ID, EVT, CH]
    extends EventStore[ID, EVT, CH] {

  /** Publish format. */
  protected type PublishTXN <: AnyRef
  /** Publish format codec. */
  protected def publishCodec: Codec[TXN, PublishTXN]

  private def publish(txn: Future[TXN]): Unit = {
    txn.foreach { txn =>
      try publish(txn.channel, publishCodec encode txn) catch {
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
  protected def publish(channel: CH, txn: PublishTXN): Unit

}
