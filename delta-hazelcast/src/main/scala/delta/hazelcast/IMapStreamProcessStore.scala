package delta.hazelcast

import java.util.Map.Entry

import delta._
import delta.process._

import com.hazelcast.core._
import com.hazelcast.logging.ILogger
import com.hazelcast.projection.Projection
import com.hazelcast.aggregation.Aggregator
import com.hazelcast.query._

import scala.concurrent._
import scala.collection.compat._
import scala.jdk.CollectionConverters._

import scuff.concurrent._
import scuff.Reduction
import scala.util.Failure

/**
  * @note This class will generally only make sense if
  * the provided `IMap` is backed by a `MapStore`.
  *
  * @param imap
  * @param logger
  * @param updateCodec
  */
abstract class IMapStreamProcessStore[K, V, U](
  imap: IMap[K, delta.Snapshot[V]],
  logger: ILogger)(
  implicit
  protected val updateCodec: UpdateCodec[V, U])
extends IMapSnapshotStore[K, V](imap, logger)
with StreamProcessStore[K, V, U]
with NonBlockingCASWrites[K, V, U] {

  def name = imap.getName

  protected def blockingBatchIO: ExecutionContext

  def readBatch(keys: Iterable[K]): Future[collection.Map[K, Snapshot]] =
    blockingBatchIO.submit {
      imap
        .getAll(SetHasAsJava(keys.toSet).asJava)
        .asScala
    }

  def writeBatch(
      batch: Iterable[(K, Snapshot)])
      : Future[WriteErrors] =
    blockingBatchIO.submit {
      imap.putAll(batch.asJava)
      NoWriteErrors
    }

  def refresh(key: K, revision: Revision, tick: Tick): Future[Unit] =
    write(key, Left(revision -> tick))

  def refreshBatch(
      revisions: collection.Map[K, (Revision, Tick)])
      : Future[WriteErrors] = {
    implicit def ec = Threads.PiggyBack
    val futures: Iterator[(K, Future[Unit])] =
      revisions.iterator
        .map {
          case (key, update) =>
            key -> write(key, Left(update))
        }
    Future.sequenceTry(futures)(_._2, _._1).map { results =>
      results
        .collect {
          case (Failure(failure), key) => key -> failure
        }
        .toMap
    }

  }

  import IMapStreamProcessStore._

  /**
   *  Write snapshot, if absent.
   *  Otherwise return present snapshot.
   *  @return `None` if write was successful, or `Some` present snapshot
   */
  protected def writeIfAbsent(
    key: K, snapshot: Snapshot): Future[Option[Snapshot]] = {

    val entryProc = new WriteIfAbsent(snapshot)
    val callback = CallbackPromise.option[Snapshot]
    imap.submitToKey(key, entryProc, callback)
    callback.future
  }

  /**
   *  Write replacement snapshot, if old snapshot matches.
   *  Otherwise return current snapshot.
   *  @return `None` if write was successful, or `Some` current snapshot
   */
  protected def writeReplacement(
    key: K, oldSnapshot: Snapshot, newSnapshot: Snapshot)
    : Future[Option[Snapshot]] = {

    val entryProc = new WriteReplacement(oldSnapshot.revision, oldSnapshot.tick, newSnapshot)
    val callback = CallbackPromise.option[Snapshot]
    imap.submitToKey(key, entryProc, callback)
    callback.future
  }

}

object IMapStreamProcessStore {

  import com.hazelcast.map.AbstractEntryProcessor
  import delta.Snapshot

  sealed abstract class Updater[K, V]
    extends AbstractEntryProcessor[K, Snapshot[V]](true)

  final case class WriteIfAbsent[K, V](snapshot: Snapshot[V])
    extends Updater[K, V] {

    def process(entry: Entry[K, Snapshot[V]]): Object =
      entry.getValue match {
        case null =>
          entry setValue snapshot
          null
        case existing =>
          existing
      }

    }

  final case class WriteReplacement[K, V](
      expectedRev: Revision, expectedTick: Tick, newSnapshot: Snapshot[V])
    extends Updater[K, V] {

    def process(entry: Entry[K, Snapshot[V]]): Object =
      entry.getValue match {
        case null => throw new IllegalStateException(s"No snapshot found!")
        case existing @ Snapshot(_, rev, tick) =>
          if (rev == expectedRev && tick == expectedTick) {
            entry setValue newSnapshot
            null
          } else existing
      }

    }

}

/**
  * @note If applying this trait, make sure the
  * `IMap` is fully loaded from the backing `MapStore`
  * ''and'' doesn't evict entries.
  */
trait IMapValueQueries[K, V, U]
extends SecondaryIndexing {
  store: IMapStreamProcessStore[K, V, U] =>

  protected def imap: IMap[K, delta.Snapshot[V]]
  protected def blockingCtx: ExecutionContext

  protected type QueryType = Comparable[_]
  protected def bulkReadBatchSize = 1000

  private def consumeAll[R](
      batch: PagingPredicate[K, Snapshot],
      consumer: Reduction[(StreamId, Snapshot), R]): R = {
    val entries = imap entrySet batch
    if (entries.isEmpty) consumer.result()
    else {
      entries.forEach { entry =>
        consumer next entry.getKey -> entry.getValue
      }
      if (entries.size < bulkReadBatchSize) consumer.result()
      else {
        batch.nextPage()
        consumeAll(batch, consumer)
      }
    }
  }

  protected def bulkRead[R](
      filter: (String, QueryType)*)(
      consumer: Reduction[(StreamId, Snapshot), R])
      : Future[R] = Future {
    val predicate =
      if (filter.isEmpty) {
        Predicates.alwaysTrue()
      } else if (filter.tail.isEmpty) {
        val (name, value) = filter.head
        Predicates.equal(name, value)
      } else {
        val predicates = filter.map {
          case (name, value) => Predicates.equal(name, value)
        }
        Predicates.and(predicates: _*)
      }
    val batch = new PagingPredicate[K, Snapshot](predicate, bulkReadBatchSize)
    consumeAll(batch, consumer)
  }(blockingCtx)

  private def query[R](
      nameValue: (String, QueryType), more: Seq[(String, QueryType)])(
      projection: Projection[Entry[K, Snapshot], (K, R)])
      : Future[Map[K, R]]  = {

    val predicate =
      if (more.isEmpty) {
        Predicates.equal(nameValue._1, nameValue._2)
      } else {
        val predicates = (nameValue +: more).map {
          case (name, value) => Predicates.equal(name, value)
        }
        Predicates.and(predicates: _*)
      }
    Future {
      blocking {
        imap
          .project(projection, predicate.asInstanceOf[Predicate[K, Snapshot]])
          .iterator.asScala.toMap
      }
    }(blockingCtx)

  }
  // protected def queryForSnapshot(
  //     nameValue: (String, QueryType), more: (String, QueryType)*)
  //     : Future[Map[StreamId, Snapshot]] =
  //   query(
  //     nameValue, more)(
  //     KeySnapshotProjection.asInstanceOf[Projection[Entry[K, Snapshot], (K, Snapshot)]])

  protected def readTicks(
      nameValue: (String, QueryType), more: (String, QueryType)*)
      : Future[Map[StreamId, Tick]] =
    query(
      nameValue, more)(
      KeyTickProjection.asInstanceOf[Projection[Entry[K, Snapshot], (K, Tick)]])

}

// object KeySnapshotProjection
// extends Projection[Entry[Any, Snapshot[Any]], (Any, Snapshot[Any])] {
//   def transform(entry: Entry[Any, Snapshot[Any]]): (Any, Snapshot[Any]) =
//     entry.getKey -> entry.getValue
// }

object KeyTickProjection
extends Projection[Entry[Any, Snapshot[Any]], (Any, Tick)] {
  def transform(entry: Entry[Any, Snapshot[Any]]): (Any, Tick) =
    entry.getKey -> entry.getValue.tick
}

/**
  * @note If applying this trait, make sure the
  * `IMap` is fully loaded from the backing `MapStore`
  * ''and'' doesn't evict entries.
  */
trait IMapAggregationSupport[K, V, U]
extends AggregationSupport {
  store: IMapStreamProcessStore[K, V, U] =>

  protected def imap: IMap[K, delta.Snapshot[V]]
  protected def blockingCtx: ExecutionContext

  protected type MetaType[R] = Projection[V, R]

  protected def findDuplicates[D](
      refName: String)(
      implicit
      projection: Projection[V, D])
      : Future[Map[D,Map[StreamId, Tick]]] = {

    val aggr = new IMapDuplicateAggregator[K, V, D](projection)
    Future {
      blocking {
        imap aggregate aggr
      }
    }(blockingCtx)
  }


}

class IMapDuplicateAggregator[K, V, D](val projection: Projection[V, D])
extends Aggregator[Entry[K, delta.Snapshot[V]], Map[D, Map[K, Tick]]] {

  private val grouped = collection.mutable.HashMap[D, Map[K, Tick]]()

  def accumulate(entry: Entry[K, delta.Snapshot[V]]): Unit = {
    val dupeKey = projection transform entry.getValue.state
    val merged = grouped.getOrElse(dupeKey, Map.empty).updated(entry.getKey, entry.getValue.tick)
    grouped.update(dupeKey, merged)
  }

  def combine(that: Aggregator[_, _]): Unit =
    that match {
      case that: IMapDuplicateAggregator[K, V, D] =>
        that.grouped.foreach {
          case (key, thatMap) =>
            val merged = grouped.getOrElse(key, Map.empty) ++ thatMap
            grouped.update(key, merged)
        }
      case _ => sys.error(s"Combining with different aggregator: ${that.getClass.getName}")
    }

  def aggregate(): Map[D, Map[K, Tick]] =
    grouped.iterator.filter(_._2.size > 1).toMap

}
