package delta.util

import delta.MessageTransport
import scala.concurrent.ExecutionContext
import scuff.Codec
import scuff.Subscription

/**
  * Local (JVM scope) transaction hub.
  */
final class LocalTransport[M](
  getTopic: M => MessageTransport.Topic,
  protected val publishCtx: ExecutionContext)
extends MessageTransport {

  type TransportType = M
  protected val messageCodec = Codec.noop[TransportType]

  protected type SubscriptionKey = Unit
  private val SubscriptionKeys = Set(())
  protected def subscriptionKeys(topics: Set[Topic]): Set[SubscriptionKey] = SubscriptionKeys
  protected def subscribeToKey(
      key: SubscriptionKey)(
      callback: (Topic, TransportType) => Unit)
      : Subscription =
    pubSub.subscribe(_ => true) { msg =>
      callback(getTopic(msg), msg)
    }

  private val pubSub = new scuff.PubSub[TransportType, TransportType](publishCtx)

  protected def publish(msg: TransportType, topic: Topic) =
    pubSub publish msg

}
