package foo

import delta.util.TransientEventStore
import scala.concurrent.ExecutionContext
import redis.clients.jedis.JedisShardInfo

import delta.Publishing
import delta.redis.RedisMessageHub
import delta.Transaction.Channel

class SomeEventStore(ec: ExecutionContext, jedisInfo: JedisShardInfo)
  extends TransientEventStore[Int, MyEvent, Array[Byte]](ec)
  with Publishing[Int, MyEvent] {

  protected def toNamespace(ch: Channel) = Namespace(s"${this.getClass}:$ch")
  protected val txnHub = new RedisMessageHub[TXN](jedisInfo, ec)
  protected val txnChannels = Set(Channel("txn"))

}
