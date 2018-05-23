package delta.redis

import scala.concurrent.{ ExecutionContext, Future, blocking }

import delta.SnapshotStore
import redis.clients.jedis.{ BinaryJedis, Pipeline }
import scuff.Serializer
import redis.clients.jedis.Transaction

/**
  * Snapshot-store using Redis
  * as back-end.
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
  protected def pipeline[R](conn: BinaryJedis = null)(thunk: Pipeline => R): R = {
    if (conn == null) jedis(pipeline(_)(thunk))
    else {
      val pl = conn.pipelined()
      try thunk(pl) finally pl.sync()
    }
  }
  protected def transaction(conn: BinaryJedis = null)(thunk: Transaction => Unit): Boolean = {
    if (conn == null) jedis(transaction(_)(thunk))
    else {
      val txn = conn.multi()
      thunk(txn)
      !txn.exec().isEmpty
    }
  }

  protected def read(conn: BinaryJedis)(binKey: Array[Byte]): Option[Snapshot] = {
    conn.hget(hash, binKey) match {
      case null => None
      case arr => Some(snapshotCodec decode arr)
    }
  }

  def read(key: K): Future[Option[Snapshot]] = Future {
    jedis(read(_)(keyCodec encode key))
  }(blockingCtx)

  protected def write(
      conn: { def hset(hash: Array[Byte], key: Array[Byte], value: Array[Byte]): Any })(
      key: K, snapshot: Snapshot): Unit = {
    import scala.language.reflectiveCalls
    val binKey = keyCodec encode key
    val binSnapshot = snapshotCodec encode snapshot
    conn.hset(hash, binKey, binSnapshot)
  }

  def write(key: K, snapshot: Snapshot): Future[Unit] = Future {
    jedis(write(_)(key, snapshot))
  }(blockingCtx)

}
