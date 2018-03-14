package foo

import delta.util.TransientEventStore
import scala.concurrent.ExecutionContext
import redis.clients.jedis.JedisShardInfo
import delta.redis.RedisPublisher

import delta.Publishing
import delta.redis.RedisEncoder

class SomeEventStore(ec: ExecutionContext, jedisInfo: JedisShardInfo)
  extends TransientEventStore[Int, MyEvent, Unit, Array[Byte]](ec)
  with Publishing[Int, MyEvent, Unit] {

  private[this] val channelName = RedisEncoder encode "redis:channel"
  private val channelEncoder: Unit => Array[Byte] = _ => channelName

  protected val publisher = new RedisPublisher[Int, MyEvent, Unit](jedisInfo, channelEncoder, Set(()), ec)

}
