package delta.hazelcast

import com.hazelcast.core.{ ITopic, Message, MessageListener }

import scuff.Subscription
import concurrent.blocking
import com.hazelcast.core.ITopic
import delta.MessageHub
import com.hazelcast.core.HazelcastInstance
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal
import delta.process.SnapshotUpdate

object TopicMessageHub {

  private def getITopic[MsgType](
      hz: HazelcastInstance)(
      topic: Topic): ITopic[MsgType] = hz.getTopic[MsgType](topic.toString)

  def apply[ID, S](
      hz: HazelcastInstance,
      publishCtx: ExecutionContext): TopicMessageHub[(ID, SnapshotUpdate[S])] =
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

  type MsgType = M

  protected def publishImpl(topic: Topic, msg: MsgType) = blocking {
    getITopic(topic).publish(msg)
  }

  protected type SubscriptionKey = Topic
  protected def subscriptionKeys(topics: Set[Topic]): Set[SubscriptionKey] = topics
  protected def subscribeToKey(topic: Topic)(callback: (Topic, MsgType) => Unit): Subscription = {
    val hzTopic = getITopic(topic)
    val regId = hzTopic addMessageListener new MessageListener[MsgType] {
      def onMessage(msg: Message[MsgType]): Unit = callback(topic, msg.getMessageObject)
    }
    new Subscription {
      def cancel(): Unit = try hzTopic.removeMessageListener(regId) catch {
        case NonFatal(th) => publishCtx.reportFailure(th)
      }
    }
  }

}
