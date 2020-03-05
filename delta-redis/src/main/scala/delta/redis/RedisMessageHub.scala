package delta.redis

import java.util.concurrent._

import scala.concurrent._, duration._
import scala.util.Try
import scala.util.control.{ NoStackTrace, NonFatal }

import delta.{ BufferedRetryPublish, MessageHub, SubscriptionPooling }
import redis.clients.jedis.{ BinaryJedis, BinaryJedisPubSub, JedisShardInfo }
import redis.clients.jedis.exceptions.JedisConnectionException
import scuff.concurrent.{ BoundedResourcePool, FailureTracker, ResourcePool, Threads, UnboundedResourcePool }
import scuff.Subscription

private object RedisMessageHub {
  val DefaultLifecycle = ResourcePool.onEviction[BinaryJedis](_.quit) {
    case _: JedisConnectionException => true
    case _ => false
  }
}

/**
 * @param info Redis server information
 * @param channelCodec
 */
class RedisMessageHub(
  info: JedisShardInfo,
  maxConnections: Int,
  protected val publishCtx: ExecutionContext,
  publishBuffer: BlockingQueue[Any],
  publishFailureThreshold: Int,
  pooledSubscriptionCancellationDelay: Option[(ScheduledExecutorService, FiniteDuration)] = None,
  failureBackoff: Iterable[FiniteDuration] = MessageHub.DefaultBackoff)(
  implicit
  lifecycle: ResourcePool.Lifecycle[BinaryJedis] = RedisMessageHub.DefaultLifecycle)
extends MessageHub
with SubscriptionPooling
with BufferedRetryPublish {

  def this(
      info: JedisShardInfo,
      maxConnections: Int,
      publishCtx: ExecutionContext,
      publishBuffer: BlockingQueue[Any],
      publishFailureThreshold: Int) =
    this(info, maxConnections, publishCtx, publishBuffer, publishFailureThreshold, None)

  @inline private def Topic(bytes: Array[Byte]) = MessageHub.Topic(RedisCodec decode bytes)
  @inline private def toRedisChannel(topic: Topic): Array[Byte] = RedisCodec encode topic.toString

  protected def cancellationDelay = pooledSubscriptionCancellationDelay

  /** The publish queue. */
  protected val publishQueue = publishBuffer.asInstanceOf[BlockingQueue[(Topic, Message)]]
  /** The threshold before circuit breaker is tripped. */
  protected def circuitBreakerThreshold = publishFailureThreshold
  /** The retry back-off schedule circuit breaker. */
  protected def publishFailureBackoff = failureBackoff

  type Message = Array[Byte]

  protected type SubscriptionKey = Set[Topic]
  protected def subscriptionKeys(topics: Set[Topic]): Set[SubscriptionKey] = Set(topics)

  protected def unusedConnectionTimeout = 10.minutes

  private[this] val jedisPool = {
    val pool = if (maxConnections == Int.MaxValue) {
      new UnboundedResourcePool(new BinaryJedis(info), 2)
    } else {
      new BoundedResourcePool(new BinaryJedis(info), 2, maxConnections)
    }
    val exe = pooledSubscriptionCancellationDelay.map(_._1) getOrElse {
      val reportFailure = publishCtx.reportFailure _
      val tf = Threads.daemonFactory(s"${getClass.getSimpleName} Redis connection evictor", reportFailure)
      Threads.newSingleRunExecutor(tf, reportFailure)
    }
    pool.startEviction(unusedConnectionTimeout, exe)
    pool
  }

  protected def publish(msg: Array[Byte], topic: Topic) = {
    jedisPool.use { jedis =>
      blocking(jedis.publish(RedisCodec.encode(topic.toString), msg))
    }
  }

  private val subscriberThreadGroup = Threads.newThreadGroup(s"${getClass.getName}:subscriber", daemon = false, publishCtx.reportFailure)

  protected def subscribeToKey(channels: SubscriptionKey)(callback: (Topic, Message) => Unit): Subscription = {
    val jedisSubscriber = new BinaryJedisPubSub {
      override def onMessage(channelBytes: Array[Byte], byteMsg: Array[Byte]): Unit = {
        callback(Topic(channelBytes), byteMsg)
      }
    }
    subscribeToChannels(channels, jedisSubscriber)
  }

  private final class ConnectionException(retryDelay: FiniteDuration)
    extends RuntimeException(s"Bad connection. Will retry in $retryDelay")
    with NoStackTrace

  private def subscribeToChannels(
      channels: SubscriptionKey,
      jedisSubscriber: BinaryJedisPubSub): Subscription = {
    val jedis = new BinaryJedis(info)
    val subKey = channels.mkString("|")
    val redisChannels = channels.toSeq.map(toRedisChannel)
    val subscriberThread = new Thread(subscriberThreadGroup, s"${getClass.getName}[$subKey]") {
      override def run() = try {
        consumeMessages()
      } catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
        case NonFatal(e) => publishCtx.reportFailure(e)
      }

      private def consumeMessages() = {
        val ft = new FailureTracker(2, publishCtx.reportFailure, failureBackoff)
        while (!Thread.currentThread.isInterrupted) {
          val timeout = ft.timeout()
          if (timeout.length > 0) {
            publishCtx reportFailure new ConnectionException(timeout)
            timeout.unit.sleep(timeout.length)
          }
          try {
            jedis.connect()
            ft.reset() // Connection successful
            jedis.subscribe(jedisSubscriber, redisChannels: _*) // forever blocking call (unless exception)
          } catch {
            case jce: JedisConnectionException =>
              ft reportFailure jce
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
