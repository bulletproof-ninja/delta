package delta.redis

import scala.concurrent.ExecutionContext

import redis.clients.jedis.BinaryJedis
import scuff.Codec

/**
  * Snapshot-store using Redis
  * as backend.
  */
class RedisSnapshotStore[K, T](
    keyCodec: Codec[K, String],
    snapshotCodec: Codec[delta.Snapshot[T], String],
    hashName: String,
    blockingCtx: ExecutionContext)(
    jedisProvider: ((BinaryJedis => Any) => Any))
  extends BinaryRedisSnapshotStore[K, T](
      Codec.concat(keyCodec, Codec.UTF8),
      Codec.concat(snapshotCodec, Codec.UTF8),
      hashName, blockingCtx)(jedisProvider) {

//  protected val hash = RedisEncoder encode hashName

//  protected def jedis[R](thunk: BinaryJedis => R): R = blocking(jedisProvider(thunk)).asInstanceOf[R]
//
//  def read(key: K): Future[Option[Snapshot]] = Future {
//    val binKey = keyCodec encode key
//    jedis(_.hget(hash, binKey)) match {
//      case null => None
//      case arr => Some(snapshotCodec decode arr)
//    }
//  }(blockingCtx)
//
//  def write(key: K, snapshot: Snapshot): Future[Unit] = Future {
//    val binKey = keyCodec encode key
//    val binSnapshot = snapshotCodec encode snapshot
//    jedis(_.hset(hash, binKey, binSnapshot))
//    ()
//  }(blockingCtx)

}
