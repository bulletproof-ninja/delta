package delta.util

import scuff.{ Codec, Subscription }
import delta.Publisher
import scala.concurrent.ExecutionContext

/**
  * Local (JVM scope) Publisher.
  */
final class LocalPublisher[ID, EVT, CH](
    protected val publishCtx: ExecutionContext)
  extends Publisher[ID, EVT, CH] {

  protected type PublishFormat = TXN
  protected def publishCodec: Codec[TXN, PublishFormat] = Codec.noop

  private[this] val pubSub = new scuff.PubSub[TXN, TXN](publishCtx)

  protected def publish(stream: ID, ch: CH, txn: PublishFormat) = pubSub.publish(txn)

  def subscribe[U](include: TXN => Boolean, callback: TXN => U, channelSubset: Set[CH]): Subscription = {
    pubSub.subscribe(include)(callback)
  }

}
