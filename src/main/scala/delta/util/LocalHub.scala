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

  type MsgType = M
  protected val messageCodec = Codec.noop[MsgType]

  protected type SubscriptionKey = Unit
  private val SubscriptionKeys = Set(())
  protected def subscriptionKeys(topics: Set[Topic]): Set[SubscriptionKey] = SubscriptionKeys
  protected def subscribeToKey(key: SubscriptionKey)(callback: (Topic, MsgType) => Unit): Subscription = {
    pubSub.subscribe(_ => true) { msg =>
      callback(getTopic(msg), msg)
    }
  }

  private val pubSub = new scuff.PubSub[MsgType, MsgType](publishCtx)

  protected def publishImpl(topic: Topic, msg: MsgType) = pubSub.publish(msg)

}
