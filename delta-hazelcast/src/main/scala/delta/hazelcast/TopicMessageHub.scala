package delta.hazelcast

import com.hazelcast.core.{ ITopic, Message, MessageListener }

import scuff.Subscription
import concurrent.blocking
import com.hazelcast.core.ITopic
import delta.MessageHub
import scuff.Codec
import com.hazelcast.core.HazelcastInstance
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

object TopicMessageHub {
  private def getTopic[PF](hz: HazelcastInstance)(ns: Namespace): ITopic[PF] = hz.getTopic[PF](ns.toString)

  def apply[MSG](
      hz: HazelcastInstance,
      publishCtx: ExecutionContext): TopicMessageHub[MSG, MSG] =
    new TopicMessageHub(hz, publishCtx, Codec.noop[MSG])

  type Namespace = MessageHub.Namespace
}

/**
 * Publishing implementation using a
 * Hazelcast `ITopic`.
 *
 * @param getTopic Return an `ITopic` for namespace.
 * @param publishCtx The execution context to publish on
 * @param messageCodec Message translation codec
 */
class TopicMessageHub[MSG, PF](
    getTopic: MessageHub.Namespace => ITopic[PF],
    protected val publishCtx: ExecutionContext,
    protected val messageCodec: Codec[MSG, PF])
  extends MessageHub[MSG] {

  def this(
      hz: HazelcastInstance,
      publishCtx: ExecutionContext,
      messageCodec: Codec[MSG, PF]) =
    this(TopicMessageHub.getTopic[PF](hz) _, publishCtx, messageCodec)

  type PublishFormat = PF

  protected def publishImpl(ns: Namespace, msg: PublishFormat) = blocking {
    getTopic(ns).publish(msg)
  }

  protected type SubscriptionKey = Namespace
  protected def subscriptionKeys(nss: Set[Namespace]): Set[SubscriptionKey] = nss
  protected def subscribeToKey(ns: Namespace)(callback: (Namespace, PF) => Unit): Subscription = {
    val topic = getTopic(ns)
    val regId = topic addMessageListener new MessageListener[PF] {
      def onMessage(msg: Message[PF]): Unit = callback(ns, msg.getMessageObject)
    }
    new Subscription {
      def cancel(): Unit = try topic.removeMessageListener(regId) catch {
        case NonFatal(th) => publishCtx.reportFailure(th)
      }
    }
  }

}
