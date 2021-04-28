package delta.hazelcast

import scala.concurrent._
import scala.util.control.NonFatal

import com.hazelcast.core._

import scuff.Subscription

import delta.MessageTransport
import delta.process.Update

object TopicMessageTransport {

  type Topic = MessageTransport.Topic

  private def getITopic[M](
      hz: HazelcastInstance)(
      topic: Topic): ITopic[M] = hz.getTopic[M](topic.toString)

  def forUpdates[ID, U](
      hz: HazelcastInstance,
      publishCtx: ExecutionContext)
      : MessageTransport[(ID, Update[U])] =
    new TopicMessageTransport(hz, publishCtx)

}

/**
 * [[delta.MessageTransport]] implementation using a
 * Hazelcast `ITopic`.
 *
 * @param getTopic Return an `ITopic` for topic.
 * @param publishCtx The execution context to publish on
 * @param messageCodec Message translation codec
 */
class TopicMessageTransport[M](
  getITopic: MessageTransport.Topic => ITopic[M],
  protected val publishCtx: ExecutionContext)
extends MessageTransport[M] {

  def this(
      hz: HazelcastInstance,
      publishCtx: ExecutionContext) =
    this(TopicMessageTransport.getITopic[M](hz) _, publishCtx)

  protected def publish(msg: M, topic: Topic) = blocking {
    getITopic(topic).publish(msg)
  }

  protected type SubscriptionKey = Topic
  protected def subscriptionKeys(topics: Set[Topic]): Set[SubscriptionKey] = topics
  protected def subscribeToKey(topic: Topic)(callback: (Topic, M) => Unit): Subscription = {
    val hzTopic = getITopic(topic)
    val regId = hzTopic addMessageListener new MessageListener[M] {
      def onMessage(msg: Message[M]): Unit = callback(topic, msg.getMessageObject)
    }
    new Subscription {
      def cancel(): Unit = try hzTopic.removeMessageListener(regId) catch {
        case NonFatal(th) => publishCtx.reportFailure(th)
      }
    }
  }

}
