package delta.redis

import redis.clients.jedis._
import scala.concurrent._, duration._
import delta.MessageHub
import scuff._
import delta.SubscriptionPooling
import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.duration.FiniteDuration
import scuff.concurrent.ResourcePool
import scuff.concurrent.Threads
import scala.util.control.NonFatal
import redis.clients.jedis.exceptions.JedisConnectionException
import scala.util.Try
import scala.util.control.NoStackTrace

private object RedisMessageHub {
  val MaxDelayInterval = 60.seconds
  val FiveSeconds = 5.seconds
  val DefaultRetryDelays = new Iterable[FiniteDuration] {
    def iterator =
      Iterator(100.millisecond, 1.second, 3.seconds) ++
        Iterator.iterate(FiveSeconds) { prev =>
          if (prev >= MaxDelayInterval) prev
          else prev + FiveSeconds
        }
  }

}

/**
 * @param info Redis server information
 * @param channelCodec
 */
class RedisMessageHub[MSG](
    info: JedisShardInfo,
    protected val publishCtx: ExecutionContext,
    protected val messageCodec: Codec[MSG, Array[Byte]],
    protected val cancellationDelay: Option[(ScheduledExecutorService, FiniteDuration)] = None,
    subscribeRetryDelay: Iterable[FiniteDuration] = RedisMessageHub.DefaultRetryDelays)
  extends MessageHub[MSG]
  with SubscriptionPooling[MSG] {

  def this(
      info: JedisShardInfo,
      publishCtx: ExecutionContext,
      publishCodec: Codec[MSG, Array[Byte]]) =
    this(info, publishCtx, publishCodec, None)

  def this(
      info: JedisShardInfo,
      publishCtx: ExecutionContext) =
    this(info, publishCtx, JavaSerializer[MSG], None)

  @inline private def Namespace(bytes: Array[Byte]) = MessageHub.Namespace(RedisCodec decode bytes)
  @inline private def toRedisChannel(ns: Namespace): Array[Byte] = RedisCodec encode ns.toString

  type PublishFormat = Array[Byte]

  protected type SubscriptionKey = Set[Namespace]
  protected def subscriptionKeys(nss: Set[Namespace]): Set[SubscriptionKey] = Set(nss)

  private[this] val jedisPool = {
    val pool = new ResourcePool(new BinaryJedis(info), 2)
    val tf = Threads.factory(s"${getClass.getSimpleName} Redis connection pruner")
    pool.startPruning(10.minutes, _.quit(), Threads.newSingleRunExecutor(tf, publishCtx.reportFailure))
    pool
  }

  protected def publishImpl(ns: Namespace, msg: Array[Byte]) = {
    jedisPool.use { jedis =>
      blocking(jedis.publish(RedisCodec.encode(ns.toString), msg))
    }
  }

  private val threadGroup = Threads.newThreadGroup(s"${getClass.getName}", daemon = false, publishCtx.reportFailure)

  protected def subscribeToKey(channels: SubscriptionKey)(callback: (Namespace, PublishFormat) => Unit): Subscription = {
    val jedisSubscriber = new BinaryJedisPubSub {
      override def onMessage(channelBytes: Array[Byte], byteMsg: Array[Byte]): Unit = {
        callback(Namespace(channelBytes), byteMsg)
      }
    }
    subscribeToChannels(channels, jedisSubscriber)
  }

  private final class ConnectionException(retryDelay: FiniteDuration, cause: JedisConnectionException)
    extends RuntimeException(s"Bad connection. Will retry in $retryDelay", cause)
    with NoStackTrace

  private def subscribeToChannels(
      channels: SubscriptionKey,
      jedisSubscriber: BinaryJedisPubSub): Subscription = {
    val jedis = new BinaryJedis(info)
    val subKey = channels.mkString("|")
    val redisChannels = channels.toSeq.map(toRedisChannel)
    val subscriberThread = new Thread(threadGroup, s"${getClass.getName}[$subKey]") {
      override def run() = try {
        consumeMessages()
      } catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
        case NonFatal(e) => publishCtx.reportFailure(e)
      }

      def consumeMessages() = {
        var retryDelays: Iterator[FiniteDuration] = null
        var currDelay: FiniteDuration = RedisMessageHub.FiveSeconds
        while (!Thread.currentThread.isInterrupted) {
          try {
            jedis.connect()
            retryDelays = null // Connection successful
            jedis.subscribe(jedisSubscriber, redisChannels: _*) // forever blocking call (unless exception)
          } catch {
            case jce: JedisConnectionException =>
              if (retryDelays == null) retryDelays = subscribeRetryDelay.iterator
              if (retryDelays.hasNext) currDelay = retryDelays.next
              publishCtx reportFailure new ConnectionException(currDelay, jce)
              currDelay.unit.sleep(currDelay.length) // Dedicated thread, ok to block
          } finally {
            Try(jedis.disconnect)
          }
        }
      }
    }
    subscriberThread.start()
    new Subscription {
      def cancel() = Try(jedisSubscriber.unsubscribe)
    }

  }

}
