package ulysses.util

import scuff.Subscription
import scala.concurrent._
import scuff.concurrent.StreamCallback
import ulysses.Publishing
import scuff.Codec

/**
 * Local (JVM) scope Publishing.
 */
trait LocalPublishing[ID, EVT, CH]
    extends Publishing[ID, EVT, CH] {

  protected type PublishTXN = TXN
  protected def publishCodec: Codec[TXN, PublishTXN] = Codec.noop

  private[this] val pubSub = new scuff.PubSub[TXN, TXN](publishCtx)

  protected def publish(ch: CH, txn: PublishTXN) = pubSub.publish(txn)

  def subscribe(selector: Selector)(
    callback: TXN => Unit): Subscription = {
    val sc = new StreamCallback[TXN] {
      def onNext(txn: TXN) = callback(txn)
      def onError(th: Throwable) = th.printStackTrace(System.err)
      def onCompleted() = ()
    }
    pubSub.subscribe(selector.include)(sc)
  }

}
