package ulysses.util

import scuff.Codec
import scala.concurrent.ExecutionContext
import scuff.Subscription
import scuff.concurrent.Threads
import ulysses.EventStore
import ulysses.Publishing
import scuff.PubSub
import scuff.concurrent.StreamCallback
import ulysses.StreamFilter

trait LocalPublishing[ID, EVT, CH]
    extends Publishing[ID, EVT, CH] {

  private[this] lazy val pubSub = new PubSub[TXN, TXN]

  protected def publish(txn: TXN) = pubSub.publish(txn)

  def subscribe(
    filter: StreamFilter[ID, EVT, CH] = StreamFilter.Everything())(
      callback: StreamCallback[TXN]): Subscription = {
    val subscription = pubSub.subscribe(filter.allowed)(callback)
    new Subscription {
      def cancel(): Unit = {
        subscription.cancel()
      }
    }
  }

}
