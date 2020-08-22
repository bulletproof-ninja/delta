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
import scala.jdk.CollectionConverters._

import scuff.concurrent._

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

  def readBatch(keys: Iterable[K]): Future[collection.Map[K, Snapshot]] = {
    implicit def ec = Threads.PiggyBack
    val futures = keys.map { key =>
      val f = this.read(key).map(_.map(key -> _))
      f
    }
    Future.sequence(futures).map(_.flatten.toMap)
  }
  def writeBatch(batch: collection.Map[K, Snapshot]): Future[Unit] = {
    implicit def ec = Threads.PiggyBack
    val futures = batch.iterator.map {
      case (key, snapshot) => write(key, snapshot)
    }
    Future.sequence(futures).map(_ => ())
  }
  def refresh(key: K, revision: Revision, tick: Tick): Future[Unit] =
    write(key, Left(revision -> tick))

  def refreshBatch(revisions: collection.Map[K, (Revision, Tick)]): Future[Unit] = {
    implicit def ec = Threads.PiggyBack
    val futures = revisions.iterator.map {
      case (key, update) => write(key, Left(update))
    }
    Future.sequence(futures).map(_ => ())
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
  protected def queryForSnapshot(
      nameValue: (String, QueryType), more: (String, QueryType)*)
      : Future[Map[StreamId, Snapshot]] =
    query(
      nameValue, more)(
      KeySnapshotProjection.asInstanceOf[Projection[Entry[K, Snapshot], (K, Snapshot)]])

  protected def queryForTick(
      nameValue: (String, QueryType), more: (String, QueryType)*)
      : Future[Map[StreamId, Tick]] =
    query(
      nameValue, more)(
      KeyTickProjection.asInstanceOf[Projection[Entry[K, Snapshot], (K, Tick)]])

}

object KeySnapshotProjection
extends Projection[Entry[Any, Snapshot[Any]], (Any, Snapshot[Any])] {
  def transform(entry: Entry[Any, Snapshot[Any]]): (Any, Snapshot[Any]) =
    entry.getKey -> entry.getValue
}

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
    }

  def aggregate(): Map[D, Map[K, Tick]] =
    grouped.iterator.filter(_._2.size > 1).toMap

}
