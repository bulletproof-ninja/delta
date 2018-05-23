package delta.redis

import scala.concurrent.ExecutionContext

import redis.clients.jedis.BinaryJedis
import scuff.Codec

/**
  * Snapshot-store using Redis
  * as back-end.
  */
class RedisStreamProcessStore[K, T](
    keyCodec: Codec[K, String],
    snapshotCodec: Codec[delta.Snapshot[T], String],
    hashName: String,
    blockingCtx: ExecutionContext)(
    jedisProvider: ((BinaryJedis => Any) => Any))
  extends BinaryRedisStreamProcessStore(
      Codec.pipe(keyCodec, Codec.UTF8),
      Codec.pipe(snapshotCodec, Codec.UTF8),
      hashName, blockingCtx)(jedisProvider)
