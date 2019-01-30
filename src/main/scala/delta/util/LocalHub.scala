package delta.util

import delta.MessageHub
import MessageHub.Namespace
import scala.concurrent.ExecutionContext
import scuff.Codec
import scuff.Subscription

/**
  * Local (JVM scope) transaction hub.
  */
final class LocalHub[MSG](getNamespace: MSG => Namespace,
    protected val publishCtx: ExecutionContext)
  extends MessageHub[MSG] {

  type PublishFormat = MSG
  protected val messageCodec = Codec.noop[PublishFormat]

  protected type SubscriptionKey = Unit
  private val SubscriptionKeys = Set(())
  protected def subscriptionKeys(channels: Set[Namespace]): Set[SubscriptionKey] = SubscriptionKeys
  protected def subscribeToKey(key: SubscriptionKey)(callback: (Namespace, PublishFormat) => Unit): Subscription = {
    pubSub.subscribe(_ => true) { msg: MSG =>
      callback(getNamespace(msg), msg)
    }
  }

  private val pubSub = new scuff.PubSub[PublishFormat, PublishFormat](publishCtx)

  protected def publishImpl(ns: Namespace, msg: PublishFormat) = pubSub.publish(msg)

}
