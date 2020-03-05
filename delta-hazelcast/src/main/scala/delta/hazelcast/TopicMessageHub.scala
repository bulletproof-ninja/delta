package delta.hazelcast

import com.hazelcast.core.{ ITopic, Message => HzMessage, MessageListener }

import scuff.Subscription
import concurrent.blocking
import com.hazelcast.core.ITopic
import delta.MessageHub
import com.hazelcast.core.HazelcastInstance
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal
import delta.process.Update

object TopicMessageHub {

  private def getITopic[Message](
      hz: HazelcastInstance)(
      topic: Topic): ITopic[Message] = hz.getTopic[Message](topic.toString)

  def apply[ID, U](
      hz: HazelcastInstance,
      publishCtx: ExecutionContext): TopicMessageHub[(ID, Update[U])] =
    new TopicMessageHub(hz, publishCtx)

  type Topic = MessageHub.Topic
}

/**
 * [[delta.MessageHub]] implementation using a
 * Hazelcast `ITopic`.
 *
 * @param getTopic Return an `ITopic` for topic.
 * @param publishCtx The execution context to publish on
 * @param messageCodec Message translation codec
 */
class TopicMessageHub[M](
    getITopic: MessageHub.Topic => ITopic[M],
    protected val publishCtx: ExecutionContext)
  extends MessageHub {

  def this(
      hz: HazelcastInstance,
      publishCtx: ExecutionContext) =
    this(TopicMessageHub.getITopic[M](hz) _, publishCtx)

  type Message = M

  protected def publish(msg: Message, topic: Topic) = blocking {
    getITopic(topic).publish(msg)
  }

  protected type SubscriptionKey = Topic
  protected def subscriptionKeys(topics: Set[Topic]): Set[SubscriptionKey] = topics
  protected def subscribeToKey(topic: Topic)(callback: (Topic, Message) => Unit): Subscription = {
    val hzTopic = getITopic(topic)
    val regId = hzTopic addMessageListener new MessageListener[Message] {
      def onMessage(msg: HzMessage[Message]): Unit = callback(topic, msg.getMessageObject)
    }
    new Subscription {
      def cancel(): Unit = try hzTopic.removeMessageListener(regId) catch {
        case NonFatal(th) => publishCtx.reportFailure(th)
      }
    }
  }

}
