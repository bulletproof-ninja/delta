package delta.redis

import redis.clients.jedis.BinaryJedis
import redis.clients.jedis.util.Pool

class JedisProvider(pool: Pool[_ <: BinaryJedis]) extends ((BinaryJedis => Any) => Any) {
  def apply(thunk: BinaryJedis => Any): Any = {
    val jedis = pool.getResource
    try {
      thunk(jedis)
    } finally {
      jedis.close()
    }
  }
}
