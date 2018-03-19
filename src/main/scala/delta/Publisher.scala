package delta

import scuff.Codec
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal
import scuff.Subscription

/**
  * Publisher of transactions.
  *
  * @tparam ID Stream id type
  * @tparam EVT Event type
  * @tparam CH Channel type
  */
abstract class Publisher[ID, EVT] {

  type TXN = Transaction[ID, EVT]
  /** Publish format. */
  protected type PublishFormat
  /** Publish format codec. */
  protected def publishCodec: Codec[TXN, PublishFormat]
  /** The execution context to publish on. */
  protected def publishCtx: ExecutionContext

  final def publish(txn: Future[TXN]): Unit = {
    txn.foreach { txn =>
      try publish(txn.stream, txn.channel, publishCodec encode txn) catch {
        case NonFatal(e) => publishCtx reportFailure e
      }
    }(publishCtx)
  }

  /**
    * Publish transaction. This will happen on
    * the `publishCtx` execution context.
    */
  protected def publish(stream: ID, channel: String, txn: PublishFormat): Unit

  /**
    * Subscribe to transactions.
    */
  def subscribe[U](include: TXN => Boolean, callback: TXN => U, channelSubset: Set[String] = Set.empty): Subscription
}
