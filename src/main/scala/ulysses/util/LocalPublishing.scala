package ulysses.util

import scuff.Codec
import scala.concurrent.ExecutionContext
import scuff.Subscription
import scuff.concurrent.Threads
import ulysses.EventStore
import ulysses.Publishing
import scuff.PubSub

trait LocalPublishing[ID, EVT, CAT]
    extends Publishing[ID, EVT, CAT] {

  private[this] lazy val pubSub = new PubSub[Transaction, Transaction](publishCtx)

  protected def publish(txn: Transaction) = pubSub.publish(txn)
  def subscribe(sub: Transaction => Unit, include: CAT => Boolean) = {
      def filter(txn: Transaction): Boolean = include(txn.category)
    pubSub.subscribe(sub, filter)
  }

}
