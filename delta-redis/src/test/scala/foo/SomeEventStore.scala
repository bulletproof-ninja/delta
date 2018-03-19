package foo

import delta.util.TransientEventStore
import scala.concurrent.ExecutionContext
import redis.clients.jedis.JedisShardInfo
import delta.redis.RedisPublisher

import delta.Publishing
import delta.redis.RedisEncoder

class SomeEventStore(ec: ExecutionContext, jedisInfo: JedisShardInfo)
  extends TransientEventStore[Int, MyEvent, Array[Byte]](ec)
  with Publishing[Int, MyEvent] {

  private[this] val channelName = RedisEncoder encode "redis:channel"
  private val channelEncoder: String => Array[Byte] = _ => channelName

  protected val publisher = new RedisPublisher[Int, MyEvent](jedisInfo, channelEncoder, Set(""), ec)

}
