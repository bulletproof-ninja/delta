package delta.redis

import redis.clients.jedis._
import scala.concurrent._, duration._
import delta.Publisher
import scuff._
import scuff.concurrent._
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.util.control.NonFatal

final class RedisPublisher[ID, EVT](
    info: JedisShardInfo,
    channelEncoder: String => Array[Byte],
    allChannels: Set[String],
    protected val publishCtx: ExecutionContext,
    protected val publishCodec: Codec[delta.Transaction[ID, EVT], Array[Byte]] = JavaSerializer[delta.Transaction[ID, EVT]])
  extends Publisher[ID, EVT] {

  def this(
      info: JedisShardInfo,
      allChannels: Array[String],
      channelEncoder: String => Array[Byte],
      publishCtx: ExecutionContext,
      publishCodec: Codec[delta.Transaction[ID, EVT], Array[Byte]]) =
    this(info, channelEncoder, allChannels.toSet, publishCtx, publishCodec)

  def this(
      info: JedisShardInfo,
      allChannels: Array[String],
      channelEncoder: String => Array[Byte],
      publishCtx: ExecutionContext) =
    this(info, channelEncoder, allChannels.toSet, publishCtx)

  protected type PublishFormat = Array[Byte]

  private val (sharedLock, exclusiveLock, activeSubscribers) = {
    val rwLock = new ReentrantReadWriteLock
    val exclusive = rwLock.writeLock
    (rwLock.readLock, exclusive, exclusive.newCondition)
  }

  private class FilteredSubscriber(include: TXN => Boolean, sub: TXN => Any) {
    def tell(txn: TXN) =
      if (include(txn)) {
        publishCtx execute new Runnable {
          def run = sub(txn)
        }
      }
  }
  private[this] val subscribers = collection.mutable.Buffer[FilteredSubscriber]()

  private[this] val jedisSubscriber = new BinaryJedisPubSub {
    override def onMessage(channel: Array[Byte], byteMsg: Array[Byte]): Unit = {
      val txn = publishCodec decode byteMsg
      sharedLock {
        subscribers.foreach(_.tell(txn))
      }
    }
  }
  private[this] val jedisPool = {
    val pool = new ResourcePool(new BinaryJedis(info), 2)
    val tf = Threads.factory(s"Jedis Connection Pruner (${getClass.getSimpleName})")
    pool.startPruning(10.minutes, _.quit(), Threads.newSingleRunExecutor(tf, publishCtx.reportFailure))
    pool
  }

  def publish(stream: ID, channel: String, txn: Array[Byte]): Unit = blocking {
    jedisPool.use { jedis =>
      jedis.publish(channelEncoder(channel), txn)
    }
  }

  def subscribe[U](include: TXN => Boolean, callback: TXN => U, channelSubset: Set[String]): Subscription = {
    val filteredSub = new FilteredSubscriber(include, callback)
    exclusiveLock {
      if (subscribers.isEmpty) {
        activeSubscribers.signal()
        startSubscriberThread()
      }
      subscribers += filteredSub
    }
    new Subscription {
      def cancel() = exclusiveLock {
        subscribers -= filteredSub
        if (subscribers.isEmpty) {
          jedisSubscriber.unsubscribe()
        }
      }
    }
  }

  private[this] val tf = Threads.factory("Redis blocking subscriber")
  protected def newSubscriberThread(r: Runnable): Thread = tf.newThread(r)

  private def startSubscriberThread(): Unit = {
    val channels = allChannels.toSeq.map(channelEncoder)
    val jedis = new BinaryJedis(info)
    val subscriberThread = this newSubscriberThread new Runnable {
      def run = while (!Thread.currentThread.isInterrupted) try {
        awaitSubscribers()
        consumeMessages()
      } catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
        case NonFatal(e) => publishCtx.reportFailure(e)
      }
      def awaitSubscribers() = exclusiveLock {
        activeSubscribers.await(subscribers.nonEmpty)
      }
      def consumeMessages() = try {
        jedis.connect()
        jedis.subscribe(jedisSubscriber, channels: _*)
      } finally {
        jedis.disconnect()
      }
    }
    subscriberThread.start()
  }

}
