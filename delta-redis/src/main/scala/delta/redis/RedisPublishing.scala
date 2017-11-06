package delta.redis

import concurrent._, duration._

import redis.clients.jedis.{ BinaryJedis, BinaryJedisPubSub, JedisShardInfo }

import java.util.concurrent.locks.ReentrantReadWriteLock
import delta.Publishing
import scuff.concurrent._
import scala.util.control.NonFatal
import scuff._

/**
  * Redis pub/sub channel.
  */
trait RedisPublishing[ID, EVT, CH]
    extends Publishing[ID, EVT, CH] {

  protected type PublishTXN = Array[Byte]
  protected def info: JedisShardInfo
  protected def allChannels: Set[CH]
  protected def channelEncoder: CH => Array[Byte]

  private val (sharedLock, exclusiveLock, activeSubscribers) = {
    val rwLock = new ReentrantReadWriteLock
    val exclusive = rwLock.writeLock
    (rwLock.readLock, exclusive, exclusive.newCondition)
  }
  private class FilteredSubscriber(selector: MonotonicSelector, sub: TXN => _) {
    def tell(txn: TXN) =
      if (selector.include(txn)) {
        publishCtx execute new Runnable {
          def run = sub(txn)
        }
      }
  }
  private[this] val subscribers = collection.mutable.Buffer[FilteredSubscriber]()

  private[this] val jedisSubscriber = new BinaryJedisPubSub {
    override def onMessage(channel: Array[Byte], byteMsg: Array[Byte]) {
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

  protected def publish(channel: CH, txn: PublishTXN): Unit = jedisPool.use { jedis =>
    blocking {
      jedis.publish(channelEncoder(channel), txn)
    }
  }

  def subscribe(
    selector: MonotonicSelector)(
      callback: TXN => Any): Subscription = {
    val filteredSub = new FilteredSubscriber(selector, callback)
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

  private def startSubscriberThread() {
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
