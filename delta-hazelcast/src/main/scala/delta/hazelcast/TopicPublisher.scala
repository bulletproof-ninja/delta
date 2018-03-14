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
  def apply[ID, EVT, CH](
      hz: HazelcastInstance,
      allChannels: Set[CH],
      publishCtx: ExecutionContext): TopicPublisher[ID, EVT, CH, Transaction[ID, EVT, CH]] =
    apply(hz, allChannels, publishCtx, Codec.noop, _.toString)

  def apply[ID, EVT, CH](
      hz: HazelcastInstance,
      allChannels: Set[CH],
      publishCtx: ExecutionContext,
      topicName: CH => String): TopicPublisher[ID, EVT, CH, Transaction[ID, EVT, CH]] =
    apply(hz, allChannels, publishCtx, Codec.noop, topicName)

  def apply[ID, EVT, CH, PF](
      hz: HazelcastInstance,
      allChannels: Set[CH],
      publishCtx: ExecutionContext,
      publishCodec: Codec[delta.Transaction[ID, EVT, CH], PF]): TopicPublisher[ID, EVT, CH, PF] =
        apply(hz, allChannels, publishCtx, publishCodec, _.toString)

  def apply[ID, EVT, CH, PF](
      hz: HazelcastInstance,
      allChannels: Set[CH],
      publishCtx: ExecutionContext,
      publishCodec: Codec[delta.Transaction[ID, EVT, CH], PF],
      topicName: CH => String): TopicPublisher[ID, EVT, CH, PF] = {
    val topics: Map[CH, ITopic[PF]] = allChannels.foldLeft(Map.empty[CH, ITopic[PF]]) {
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
class TopicPublisher[ID, EVT, CH, PF](
    topics: Map[CH, ITopic[PF]],
    protected val publishCtx: ExecutionContext,
    protected val publishCodec: Codec[delta.Transaction[ID, EVT, CH], PF])
  extends Publisher[ID, EVT, CH] {

  type PublishFormat = PF

  protected def publish(stream: ID, channel: CH, txn: PublishFormat): Unit =
    blocking {
      topics(channel).publish(txn)
    }

  private class Subscriber(include: TXN => Boolean, callback: TXN => _)
    extends MessageListener[PublishFormat] {
    def onMessage(msg: Message[PublishFormat]): Unit = {
      val txn = publishCodec decode msg.getMessageObject
      if (include(txn)) callback(txn)
    }
  }

  def subscribe[U](include: TXN => Boolean, callback: TXN => U, channelSubset: Set[CH]): Subscription = {
    val channels = if (channelSubset.isEmpty) topics.keys else channelSubset
    val topicRegistrations = channels.toList.map { ch =>
      val topic = topics(ch)
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
