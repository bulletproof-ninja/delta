package delta.redis

import _root_.redis.clients.jedis.BinaryJedis

import scala.jdk.CollectionConverters._
import scala.collection.immutable.HashMap
import scala.concurrent.{ ExecutionContext, Future }

import scuff._
import java.util.concurrent.atomic.AtomicLong
import delta.process.{ StreamProcessStore, BlockingCASWrites }
import delta.process.UpdateCodec

/**
  * Binary Redis implementation of [[delta.util.StreamProcessStore]]
  */
class BinaryRedisStreamProcessStore[K, T, U](
    keyCodec: Serializer[K],
    snapshotCodec: Serializer[delta.Snapshot[T]],
    hashName: String,
    protected val blockingCtx: ExecutionContext)(
    jedisProvider: ((BinaryJedis => Any) => Any))(
    implicit
    protected val updateCodec: UpdateCodec[T, U])
  extends BinaryRedisSnapshotStore(keyCodec, snapshotCodec, hashName, blockingCtx)(jedisProvider)
  with StreamProcessStore[K, T, U] with BlockingCASWrites[K, T, U, BinaryJedis] {

  private def newJUHashMap(size: Int) = new java.util.HashMap[Array[Byte], Array[Byte]]((size / 0.67f).toInt, 0.67f)

  private[this] val lastTickWritten = new AtomicLong(Long.MinValue)

  @annotation.tailrec
  private def updateLastTickWrittenIfNeeded(newTick: Long): Unit = {
    val lastWritten = lastTickWritten.get
    if (newTick > lastWritten) {
      if (!lastTickWritten.compareAndSet(lastWritten, newTick)) {
        updateLastTickWrittenIfNeeded(newTick)
      }
    }
  }

  private[this] val TickKey = Array[Byte]('t')

  def tickWatermark: Option[Long] = {
    val tick = jedis { conn =>
      Option(conn.hget(hash, TickKey)).map(_.utf8.toLong)
    }
    tick.foreach(updateLastTickWrittenIfNeeded)
    tick
  }

  override def write(key: K, snapshot: Snapshot): Future[Unit] = {
    if (snapshot.tick <= lastTickWritten.get) {
      super.write(key, snapshot)
    } else Future {
      pipeline() { conn =>
        write(conn)(key, snapshot)
        conn.hset(hash, TickKey, snapshot.tick.toString.utf8)
      }
      updateLastTickWrittenIfNeeded(snapshot.tick)
    }(blockingCtx)
  }

  def readBatch(keys: Iterable[K]): Future[collection.Map[K, Snapshot]] = Future {
    val keysSeq = keys.toSeq
    val binKeys = keysSeq.map(keyCodec.encode)
    jedis(_.hmget(hash, binKeys: _*))
      .iterator.asScala
      .zip(keysSeq.iterator)
      .filter(_._1 != null)
      .foldLeft(HashMap.empty[K, Snapshot]) {
        case (map, (valBytes, key)) =>
          map.updated(key, snapshotCodec decode valBytes)
      }
  }(blockingCtx)

  def writeBatch(snapshots: collection.Map[K, Snapshot]): Future[Unit] = {
    if (snapshots.isEmpty) Future successful (())
    else Future {
      val (jmap, maxTick) = snapshots.iterator.foldLeft(newJUHashMap(snapshots.size) -> Long.MinValue) {
        case ((jmap, maxTick), (key, snapshot)) =>
          jmap.put(keyCodec encode key, snapshotCodec encode snapshot)
          jmap -> (maxTick max snapshot.tick)
      }
      if (maxTick > lastTickWritten.get) {
        pipeline() { conn =>
          conn.hmset(hash, jmap)
          conn.hset(hash, TickKey, maxTick.toString.utf8)
        }
        updateLastTickWrittenIfNeeded(maxTick)
      } else jedis { conn =>
        conn.hmset(hash, jmap); ()
      }
    }(blockingCtx)
  }

  protected def refreshKey(conn: BinaryJedis)(key: K, revision: Int, tick: Long): Unit = {
    val binKey = keyCodec encode key
    val snapshot = snapshotCodec decode conn.hget(hash, binKey)
    val revised = snapshot.copy(revision = revision, tick = tick)
    conn.hset(hash, binKey, snapshotCodec encode revised)
  }

  def refresh(key: K, revision: Int, tick: Long): Future[Unit] = Future {
    jedis { conn =>
      refreshKey(conn)(key, revision, tick)
    }
  }(blockingCtx)

  private def refreshBatch(conn: BinaryJedis)(binKeys: Seq[Array[Byte]], revisions: Seq[(Int, Long)]): Unit = {
    conn.watch(binKeys: _*)
    val existing = conn.hmget(hash, binKeys: _*)
    val (toRefresh, maxTick) =
      existing.iterator.asScala
        .zip(binKeys.iterator)
        .zip(revisions.iterator)
        .filter(_._1._1 != null)
        .foldLeft(newJUHashMap(revisions.size) -> Long.MinValue) {
          case ((jmap, maxTick), ((binVal, binKey), (rev, tick))) =>
            val snapshot = snapshotCodec decode binVal
            if (rev > snapshot.revision || tick > snapshot.tick) {
              val refreshed = snapshot.copy(revision = rev, tick = tick)
              jmap.put(binKey, snapshotCodec encode refreshed)
            }
            jmap -> (maxTick max snapshot.tick)
        }
    if (toRefresh.isEmpty) {
      conn.unwatch()
    } else {
      val updateMaxTick = maxTick > lastTickWritten.get
      val success = transaction(conn) { tx =>
        tx.hmset(hash, toRefresh)
        if (updateMaxTick) {
          tx.hset(hash, TickKey, maxTick.toString.utf8)
        }
      }
      if (success) {
        if (updateMaxTick) updateLastTickWrittenIfNeeded(maxTick)
      } else { // retry
        refreshBatch(conn)(binKeys, revisions)
      }

    }

  }

  def refreshBatch(revisions: collection.Map[K, (Int, Long)]): Future[Unit] = {
    if (revisions.isEmpty) Future successful (())
    else {
      var binKeys: List[Array[Byte]] = Nil
      var revTicks: List[(Int, Long)] = Nil
      revisions.foreach {
        case (key, revTickTuple) =>
          binKeys ::= keyCodec encode key
          revTicks ::= revTickTuple
      }
      Future {
        jedis { conn =>
          refreshBatch(conn)(binKeys, revTicks)
        }
      }(blockingCtx)
    }
  }

  protected def readForUpdate[R](key: K)(updateThunk: (BinaryJedis, Option[Snapshot]) => R): R = {
    jedis { conn =>
      val binKey = keyCodec encode key
      conn.watch(binKey)
      val existing = read(conn)(binKey)
      updateThunk(conn, existing)
    }
  }
  protected def writeIfAbsent(conn: BinaryJedis)(key: K, snapshot: Snapshot): Option[Snapshot] = {
    val binKey = keyCodec encode key
    val binSnapshot = snapshotCodec encode snapshot
    conn.hsetnx(hash, binKey, binSnapshot).intValue match {
      case 0 => read(conn)(binKey)
      case 1 => None
    }
  }
  protected def writeReplacement(conn: BinaryJedis)(key: K, oldSnapshot: Snapshot, newSnapshot: Snapshot): Option[Snapshot] = {
    val binKey = keyCodec encode key
    val binSnapshot = snapshotCodec encode newSnapshot
    assert(conn.getClient.isInWatch)
    val success = transaction(conn) { tx =>
      tx.hset(hash, binKey, binSnapshot)
    }
    if (success) None
    else {
      conn.watch(binKey)
      read(conn)(binKey)
    }
  }

}
