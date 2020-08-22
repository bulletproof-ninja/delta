package delta.testing

import scala.collection.compat._
import scala.concurrent._

import delta._
import delta.process._

import scuff.concurrent._
import scala.collection.concurrent.{ Map => CMap, TrieMap }
import scuff.reflect.Surgeon

abstract class InMemoryProcStore[K, S <: AnyRef, U](
  val name: String,
  protected val snapshots: CMap[K, delta.Snapshot[S]] = TrieMap.empty[K, delta.Snapshot[S]])(
  implicit
  protected val updateCodec: UpdateCodec[S, U])
extends StreamProcessStore[K, S, U]
with BlockingCASWrites[K, S, U, Unit]
with SecondaryIndexing
with AggregationSupport {

  implicit def ec = RandomDelayExecutionContext

  protected type MetaType[V] = (String, S) => Set[V]

  protected def findDuplicates[V](
      refName: String)(
      implicit
      getValue: MetaType[V])
      : Future[Map[V,Map[K,delta.Tick]]] = Future {
    snapshots
      .flatMap {
        case (key, snapshot) => getValue(refName, snapshot.state).map(v => (v, key, snapshot.tick))
      }
      .groupBy(_._1)
      .view
        .filter(_._2.size > 1)
        .mapValues(_.map(t => t._2 -> t._3).toMap)
      .toMap
  }

  type QueryType = Any
  protected def isQueryMatch(name: String, value: QueryType, state: S): Boolean = {
    val surgeon = new Surgeon(state)
    surgeon.selectDynamic(name) == value
  }
  protected def queryForSnapshot(
      nameValue: (String, QueryType), more: (String, QueryType)*)
      : Future[Map[K, Snapshot]] = Future {

    val filters = nameValue :: more.toList

    snapshots
      .filter {
        case (_, Snapshot(state, _, _)) =>
          filters.forall {
            case (name, expected) => isQueryMatch(name, expected, state)
          }
      }.toMap

    }

  protected def queryForTick(
      nameValue: (String, QueryType), more: (String, QueryType)*)
      : Future[Map[K, Long]] =
    queryForSnapshot(nameValue, more: _*)
      .map(_.view.mapValues(_.tick).toMap)

  def read(key: K): scala.concurrent.Future[Option[Snapshot]] = Future {
    snapshots.get(key)
  }
  def write(key: K, snapshot: Snapshot): scala.concurrent.Future[Unit] = Future {
    snapshots.update(key, snapshot)
  }

  def readBatch(keys: Iterable[K]): Future[Map[K, Snapshot]] = Future {
    snapshots.view.filterKeys(keys.toSet).toMap
  }
  def writeBatch(batch: scala.collection.Map[K, Snapshot]): Future[Unit] = Future {
    batch.foreach {
      case (key, snapshot) => snapshots.update(key, snapshot)
    }
  }
  def refresh(key: K, revision: Revision, tick: Tick): Future[Unit] = Future {
    refreshKey(())(key, revision, tick)
  }
  def refreshBatch(revisions: scala.collection.Map[K, (Revision, Tick)]): Future[Unit] = Future {
    revisions.foreach {
      case (key, (revision, tick)) =>
        snapshots.updateIfPresent(key)(_.copy(tick = tick, revision = revision))
    }
  }
  def tickWatermark: Option[Tick] = None
  protected def readForUpdate[R](key: K)(thunk: (Unit, Option[Snapshot]) => R): Future[R] = Future {
    thunk((), snapshots.get(key))
  }

  protected def refreshKey(conn: Unit)(key: K, revision: Revision, tick: Tick): Unit =
    snapshots.updateIfPresent(key)(_.copy(tick = tick, revision = revision))

  protected def writeIfAbsent(conn: Unit)(key: K, snapshot: Snapshot): Option[Snapshot] =
    snapshots.putIfAbsent(key, snapshot)

  protected def writeReplacement(conn: Unit)(key: K, oldSnapshot: Snapshot, newSnapshot: Snapshot): Option[Snapshot] =
    if (snapshots.replace(key, oldSnapshot, newSnapshot)) None
    else snapshots.get(key) match {
      case None => snapshots.putIfAbsent(key, newSnapshot)
      case existing => existing
    }

}
