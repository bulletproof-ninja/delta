package delta.util

import delta.MessageHub
import scala.concurrent.ExecutionContext
import scuff.Codec
import scuff.Subscription

/**
  * Local (JVM scope) transaction hub.
  */
final class LocalHub[M](getTopic: M => MessageHub.Topic,
    protected val publishCtx: ExecutionContext)
  extends MessageHub {

  type Message = M
  protected val messageCodec = Codec.noop[Message]

  protected type SubscriptionKey = Unit
  private val SubscriptionKeys = Set(())
  protected def subscriptionKeys(topics: Set[Topic]): Set[SubscriptionKey] = SubscriptionKeys
  protected def subscribeToKey(key: SubscriptionKey)(callback: (Topic, Message) => Unit): Subscription = {
    pubSub.subscribe(_ => true) { msg =>
      callback(getTopic(msg), msg)
    }
  }

  private val pubSub = new scuff.PubSub[Message, Message](publishCtx)

  protected def publish(msg: Message, topic: Topic) = pubSub.publish(msg)

}
