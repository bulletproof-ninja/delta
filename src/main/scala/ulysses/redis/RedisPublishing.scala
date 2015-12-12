package ulysses.redis

import scuff.redis._
import redis.clients.jedis.JedisShardInfo
import scuff.Codec
import scala.concurrent.ExecutionContext
import scuff.Subscription
import scuff.concurrent.Threads
import ulysses.EventStore
import ulysses.Publishing

trait RedisPublishing[ID, EVT, CAT]
    extends Publishing[ID, EVT, CAT] {

  protected def publishPool: RedisConnectionPool
  protected def subscribeServer: JedisShardInfo

  protected trait Categorizer {
    /** Name of category. */
    def name(cat: CAT): String
    /** Index of category. Must be between 0 and n-1, where n is the number of categories. */
    def index(cat: CAT): Int
  }
  protected def publishChannelName(cat: CAT): String = s"EventStore:${categorizer.name(cat)}"
  protected def publishCodec: Codec[Transaction, Array[Byte]]
  protected def allCategories: Set[CAT]
  private lazy val defaultCategorizer = {
    if (allCategories.forall(_.isInstanceOf[java.lang.Enum[_]])) {
      new Categorizer {
        def name(cat: CAT): String = cat.asInstanceOf[java.lang.Enum[_]].name
        def index(cat: CAT): Int = cat.asInstanceOf[java.lang.Enum[_]].ordinal
      }
    } else if (allCategories.forall(_.isInstanceOf[Enumeration#Value])) {
      new Categorizer {
        def name(cat: CAT): String = cat.toString
        def index(cat: CAT): Int = cat.asInstanceOf[Enumeration#Value].id
      }
    } else {
      new Categorizer {
        val _categoryIndex = allCategories.toSeq.zipWithIndex.toMap
        def name(cat: CAT): String = cat.toString
        def index(cat: CAT): Int = _categoryIndex(cat)
      }
    }

  }
  protected def categorizer: Categorizer = defaultCategorizer

  private lazy val pubChannels = {
    val channels = new Array[BinaryRedisPublisher[Transaction]](allCategories.size)
    allCategories.foreach { category =>
      channels(categorizer.index(category)) = new BinaryRedisPublisher(publishChannelName(category), publishCodec)
    }
    channels
  }
  protected def publish(txn: Transaction) = {
    val channel = pubChannels(categorizer.index(txn.category))
    publishPool { conn =>
      channel.publish(txn)(conn)
    }
  }
  private lazy val subChannels = {
    val tg = new ThreadGroup("RedisChannelSubscribers")
    allCategories.toList.map { category =>
      val channelName = publishChannelName(category)
      val tf = Threads.daemonFactory(s"RedisChannelSubscriber($channelName)", tg)
      category -> BinaryRedisFaucet(channelName, subscribeServer, tf, publishCodec, ExecutionContext.global)
    }
  }
  def subscribe(sub: Transaction => Unit, include: CAT => Boolean) = {
    val subscriptions = subChannels.filter(c => include(c._1)).map(_._2.subscribe(sub))
    new Subscription {
      def cancel {
        var cancelE: Exception = null
        subscriptions.foreach { sub =>
          try {
            sub.cancel()
          } catch {
            case e: Exception => if (cancelE == null) cancelE = e
          }
        }
        if (cancelE != null) throw cancelE
      }
    }
  }

}
