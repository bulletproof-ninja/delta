package foo

import delta.util.TransientEventStore
import scala.concurrent.ExecutionContext
import redis.clients.jedis.JedisShardInfo

import delta.MessageTransportPublishing
import delta.redis.RedisMessageTransport
import delta.Channel
import java.util.concurrent.ArrayBlockingQueue
import scuff.JavaSerializer
import delta.{ Ticker, EventSource }

class SomeEventStore(
    ec: ExecutionContext, jedisInfo: JedisShardInfo)(
    initTicker: EventSource[Int, MyEvent] => Ticker)
  extends TransientEventStore[Int, MyEvent, Array[Byte]](ec, BinaryEventFormat)(initTicker)
  with MessageTransportPublishing[Int, MyEvent] {

  protected def toTopic(ch: Channel) = Topic(s"${this.getClass}:$ch")
  private val maxConnections = Runtime.getRuntime.availableProcessors * 2
  private val buffer = new ArrayBlockingQueue[Any](2048)
  protected val txTransport = new RedisMessageTransport(jedisInfo, maxConnections, ec, buffer, 3)
  protected val txChannels = Set(Channel("tx"))
  protected val txCodec = JavaSerializer[Transaction]

}
