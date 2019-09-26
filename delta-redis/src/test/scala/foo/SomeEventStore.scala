package foo

import delta.util.TransientEventStore
import scala.concurrent.ExecutionContext
import redis.clients.jedis.JedisShardInfo

import delta.MessageHubPublishing
import delta.redis.RedisMessageHub
import delta.Transaction.Channel
import java.util.concurrent.ArrayBlockingQueue
import scuff.JavaSerializer
import delta.{ Ticker, EventSource }

class SomeEventStore(
    ec: ExecutionContext, jedisInfo: JedisShardInfo)(
    initTicker: EventSource[Int, MyEvent] => Ticker)
  extends TransientEventStore[Int, MyEvent, Array[Byte]](ec, BinaryEventFormat)(initTicker)
  with MessageHubPublishing[Int, MyEvent] {

  protected def toTopic(ch: Channel) = Topic(s"${this.getClass}:$ch")
  private val maxConnections = Runtime.getRuntime.availableProcessors * 2
  private val buffer = new ArrayBlockingQueue[Any](2048)
  protected val txnHub = new RedisMessageHub(jedisInfo, maxConnections, ec, buffer, 3)
  protected val txnChannels = Set(Channel("txn"))
  protected val txnCodec = JavaSerializer[TXN]

}
