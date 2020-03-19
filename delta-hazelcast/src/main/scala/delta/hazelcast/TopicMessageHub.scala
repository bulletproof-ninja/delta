package delta.hazelcast

import com.hazelcast.core.{ ITopic, Message => HzMessage, MessageListener }

import scuff.Subscription
import concurrent.blocking
import com.hazelcast.core.ITopic
import delta.MessageTransport
import com.hazelcast.core.HazelcastInstance
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal
import delta.process.Update

object TopicMessageTransport {

  type Topic = MessageTransport.Topic

  private def getITopic[M](
      hz: HazelcastInstance)(
      topic: Topic): ITopic[M] = hz.getTopic[M](topic.toString)

  def apply[ID, U](
      hz: HazelcastInstance,
      publishCtx: ExecutionContext)
      : MessageTransport { type TransportType = (ID, Update[U]) } =
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
  extends MessageTransport {

  def this(
      hz: HazelcastInstance,
      publishCtx: ExecutionContext) =
    this(TopicMessageTransport.getITopic[M](hz) _, publishCtx)

  type TransportType = M

  protected def publish(msg: TransportType, topic: Topic) = blocking {
    getITopic(topic).publish(msg)
  }

  protected type SubscriptionKey = Topic
  protected def subscriptionKeys(topics: Set[Topic]): Set[SubscriptionKey] = topics
  protected def subscribeToKey(topic: Topic)(callback: (Topic, M) => Unit): Subscription = {
    val hzTopic = getITopic(topic)
    val regId = hzTopic addMessageListener new MessageListener[M] {
      def onMessage(msg: HzMessage[M]): Unit = callback(topic, msg.getMessageObject)
    }
    new Subscription {
      def cancel(): Unit = try hzTopic.removeMessageListener(regId) catch {
        case NonFatal(th) => publishCtx.reportFailure(th)
      }
    }
  }

}
