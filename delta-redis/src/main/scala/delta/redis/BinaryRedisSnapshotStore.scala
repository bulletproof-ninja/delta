package delta.redis

import _root_.redis.clients.jedis.BinaryJedis

import scala.concurrent.{ ExecutionContext, Future, blocking }

import delta.SnapshotStore
import scuff.Serializer

/**
  * Snapshot-store using Redis
  * as backend.
  */
class BinaryRedisSnapshotStore[K, T](
    keyCodec: Serializer[K],
    snapshotCodec: Serializer[delta.Snapshot[T]],
    hashName: String,
    blockingCtx: ExecutionContext)(
    jedisProvider: ((BinaryJedis => Any) => Any))
  extends SnapshotStore[K, T] {

  protected val hash = RedisEncoder encode hashName

  protected def jedis[R](thunk: BinaryJedis => R): R = blocking(jedisProvider(thunk)).asInstanceOf[R]

  def read(key: K): Future[Option[Snapshot]] = Future {
    val binKey = keyCodec encode key
    jedis(_.hget(hash, binKey)) match {
      case null => None
      case arr => Some(snapshotCodec decode arr)
    }
  }(blockingCtx)

  def write(key: K, snapshot: Snapshot): Future[Unit] = Future {
    val binKey = keyCodec encode key
    val binSnapshot = snapshotCodec encode snapshot
    jedis(_.hset(hash, binKey, binSnapshot))
    ()
  }(blockingCtx)

}
