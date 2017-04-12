package delta.util

import scuff.{ Codec, Subscription }
import delta.Publishing

/**
  * Local (JVM scope) Publishing.
  */
trait LocalPublishing[ID, EVT, CH]
    extends Publishing[ID, EVT, CH] {

  protected type PublishTXN = TXN
  protected def publishCodec: Codec[TXN, PublishTXN] = Codec.noop

  private[this] val pubSub = new scuff.PubSub[TXN, TXN](publishCtx)

  protected def publish(ch: CH, txn: PublishTXN) = pubSub.publish(txn)

  def subscribe(selector: MonotonicSelector)(
    callback: TXN => Unit): Subscription = pubSub.subscribe(selector.include)(callback)

}
