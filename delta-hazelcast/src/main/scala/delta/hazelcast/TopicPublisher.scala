package delta.hazelcast

import scala.util.Try

import com.hazelcast.core.{ ITopic, Message, MessageListener }

import scuff.Subscription
import concurrent.blocking
import com.hazelcast.core.ITopic
import delta.Transaction
import delta.Publisher
import scuff.Codec
import com.hazelcast.core.HazelcastInstance
import scala.concurrent.ExecutionContext

object TopicPublisher {
  def apply[ID, EVT](
      hz: HazelcastInstance,
      allChannels: Set[String],
      publishCtx: ExecutionContext): TopicPublisher[ID, EVT, Transaction[ID, EVT]] =
    apply(hz, allChannels, publishCtx, Codec.noop, _.toString)

  def apply[ID, EVT](
      hz: HazelcastInstance,
      allChannels: Set[String],
      publishCtx: ExecutionContext,
      topicName: String => String): TopicPublisher[ID, EVT, Transaction[ID, EVT]] =
    apply(hz, allChannels, publishCtx, Codec.noop, topicName)

  def apply[ID, EVT, PF](
      hz: HazelcastInstance,
      allChannels: Set[String],
      publishCtx: ExecutionContext,
      publishCodec: Codec[delta.Transaction[ID, EVT], PF]): TopicPublisher[ID, EVT, PF] =
        apply(hz, allChannels, publishCtx, publishCodec, _.toString)

  def apply[ID, EVT, PF](
      hz: HazelcastInstance,
      allChannels: Set[String],
      publishCtx: ExecutionContext,
      publishCodec: Codec[delta.Transaction[ID, EVT], PF],
      topicName: String => String): TopicPublisher[ID, EVT, PF] = {
    val topics: Map[String, ITopic[PF]] = allChannels.foldLeft(Map.empty[String, ITopic[PF]]) {
      case (topics, ch) => topics.updated(ch, hz.getTopic(topicName(ch)))
    }
    new TopicPublisher(topics, publishCtx, publishCodec)
  }
}

/**
  * Publishing implementation using a
  * Hazelcast `ITopic`.
  *
  */
class TopicPublisher[ID, EVT, PF](
    channelTopics: Map[String, ITopic[PF]],
    protected val publishCtx: ExecutionContext,
    protected val publishCodec: Codec[delta.Transaction[ID, EVT], PF])
  extends Publisher[ID, EVT] {

  type PublishFormat = PF

  protected def publish(stream: ID, channel: String, txn: PublishFormat): Unit =
    blocking {
      channelTopics(channel).publish(txn)
    }

  private class Subscriber(include: TXN => Boolean, callback: TXN => _)
    extends MessageListener[PublishFormat] {
    def onMessage(msg: Message[PublishFormat]): Unit = {
      val txn = publishCodec decode msg.getMessageObject
      if (include(txn)) callback(txn)
    }
  }

  def subscribe[U](include: TXN => Boolean, callback: TXN => U, channelSubset: Set[String]): Subscription = {
    val channels = if (channelSubset.isEmpty) channelTopics.keys else channelSubset
    val topicRegistrations = channels.toList.map { ch =>
      val topic = channelTopics(ch)
      val regId = topic addMessageListener new Subscriber(include, callback)
      topic -> regId
    }
    new Subscription {
      def cancel() = topicRegistrations.foreach {
        case (topic, regId) => Try {
          topic removeMessageListener regId
        }
      }
    }
  }

}
