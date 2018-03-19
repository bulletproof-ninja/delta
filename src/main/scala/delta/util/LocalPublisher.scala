package delta.util

import scuff.{ Codec, Subscription }
import delta.Publisher
import scala.concurrent.ExecutionContext

/**
  * Local (JVM scope) Publisher.
  */
final class LocalPublisher[ID, EVT](
    protected val publishCtx: ExecutionContext)
  extends Publisher[ID, EVT] {

  protected type PublishFormat = TXN
  protected def publishCodec: Codec[TXN, PublishFormat] = Codec.noop

  private[this] val pubSub = new scuff.PubSub[TXN, TXN](publishCtx)

  protected def publish(stream: ID, channel: String, txn: PublishFormat) = pubSub.publish(txn)

  def subscribe[U](include: TXN => Boolean, callback: TXN => U, channelSubset: Set[String]): Subscription = {
    pubSub.subscribe(include)(callback)
  }

}
