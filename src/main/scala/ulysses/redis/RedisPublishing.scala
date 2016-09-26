package ulysses.redis

import redis.clients.jedis.JedisShardInfo
import scuff.redis._
import scuff.Codec
import scala.concurrent.ExecutionContext
import scuff.Subscription
import scuff.concurrent.Threads
import ulysses.EventStore
import ulysses.Publishing
import redis.clients.jedis.Jedis
import ulysses.StreamFilter
import scuff.concurrent.StreamCallback
import scala.util.{ Try, Failure }
import scuff.concurrent.HashPartitionExecutionContext

trait RedisPublishing[ID, EVT, CH]
    extends Publishing[ID, EVT, CH] {

  protected def jedis(thunk: Jedis => Unit): Unit
  protected def subscribeServer: JedisShardInfo

  protected trait ChannelIndexer {
    /** Name of channel. */
    def name(ch: CH): String
    /** Index of channel. Must be between 0 and n-1, where n is the number of channels. */
    def index(ch: CH): Int
  }
  protected def publishCtx: HashPartitionExecutionContext = HashPartitionExecutionContext.global
  protected def publishChannelName(ch: CH): String = s"${getClass.getSimpleName}:${indexer.name(ch)}"
  protected def publishCodec: Codec[TXN, Array[Byte]]
  protected def allChannels: Set[CH]
  private lazy val defaultIndexer = {
    if (allChannels.forall(_.isInstanceOf[java.lang.Enum[_]])) {
      new ChannelIndexer {
        def name(ch: CH): String = ch.asInstanceOf[java.lang.Enum[_]].name
        def index(ch: CH): Int = ch.asInstanceOf[java.lang.Enum[_]].ordinal
      }
    } else if (allChannels.forall(_.isInstanceOf[Enumeration#Value])) {
      new ChannelIndexer {
        def name(ch: CH): String = ch.toString
        def index(ch: CH): Int = ch.asInstanceOf[Enumeration#Value].id
      }
    } else {
      new ChannelIndexer {
        val _channelIndex = allChannels.toSeq.zipWithIndex.toMap
        def name(ch: CH): String = ch.toString
        def index(ch: CH): Int = _channelIndex(ch)
      }
    }

  }
  protected def indexer: ChannelIndexer = defaultIndexer

  private lazy val pubChannels = {
    val channels = new Array[BinaryRedisPublisher[TXN]](allChannels.size)
    allChannels.foreach { channel =>
      channels(indexer.index(channel)) =
        new BinaryRedisPublisher(publishChannelName(channel), publishCodec)
    }
    channels
  }
  protected def publish(txn: TXN) = {
    val channel = pubChannels(indexer.index(txn.channel))
    jedis { conn =>
      channel.publish(txn)(conn)
    }
  }
  private lazy val subChannels = {
    val tg = new ThreadGroup("RedisChannelSubscribers")
    allChannels.toList.map { channel =>
      val channelName = publishChannelName(channel)
      val tf = Threads.daemonFactory(s"RedisChannelSubscriber($channelName)", tg)
      channel -> BinaryRedisFeed(channelName, subscribeServer, tf, publishCodec, publishCtx)
    }
  }
  def subscribe(
    filter: StreamFilter[ID, EVT, CH])(
      callback: StreamCallback[TXN]): Subscription = {
    import StreamFilter._
    val channels = filter match {
      case Everything() =>
        subChannels
      case ByChannel(channels) =>
        subChannels.filter(cf => channels.contains(cf._1))
      case ByEvent(events) =>
        val channels = events.keySet
        subChannels.filter(cf => channels.contains(cf._1))
      case ByStream(id, channel) =>
        subChannels.filter(cf => cf._1 == channel)
    }
    val subscriptions = channels.map {
      case (_, feed) =>
        feed.subscribe(filter.allowed _)(callback.onNext)
    }
    new Subscription {
      def cancel {
        subscriptions.map(s => Try(s.cancel)).foreach {
          case Failure(th) => throw th
          case _ => // All good
        }
      }
    }
  }

}
