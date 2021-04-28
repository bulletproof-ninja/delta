package delta.util

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

import delta.MessageTransport
import scuff.Subscription
import scuff.concurrent.MultiMap

/**
  * JVM instance message transport.
  *
  * @param publishCtx The execution context used to notify subscribers
  */
class LocalTransport[M](
  protected val publishCtx: ExecutionContext)
extends MessageTransport[M] {

  protected def publish(msg: M, topic: Topic): Unit =
    publishCtx execute new Runnable {
      override def hashCode = topic.##
      def run =
        subscribers(topic).foreach { sub =>
          try sub(topic, msg) catch {
            case NonFatal(cause) =>
              publishCtx reportFailure cause
          }
        }
    }

  protected type SubscriptionKey = Topic

  private[this] val subscribers = new MultiMap[Topic, Callback]

  protected def subscriptionKeys(topics: Set[Topic]): Set[SubscriptionKey] = topics

  protected def subscribeToKey(
      topic: Topic)(
      callback: Callback)
      : Subscription = {

    subscribers.add(topic, callback)
    new Subscription {
      def cancel(): Unit =
        subscribers.remove(topic, callback)
    }
  }

}
