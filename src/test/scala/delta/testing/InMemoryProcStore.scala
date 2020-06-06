package delta.testing

import scala.collection.compat._
import scala.concurrent._

import delta._
import delta.process._

import scuff.concurrent._
import delta.validation.IndexedStore

abstract class InMemoryProcStore[K, S, U](
  protected val snapshots: collection.concurrent.Map[K, delta.Snapshot[S]],
  val name: String)(
  implicit
  protected val updateCodec: UpdateCodec[S, U])
extends StreamProcessStore[K, S, U]
with BlockingCASWrites[K, S, U, Unit]
with IndexedStore
with SecondaryIndex {

  implicit def ec = RandomDelayExecutionContext

  protected type Ref[V] = (String, S) => Set[V]

  protected def findDuplicates[V](refName: String)(implicit extract: Ref[V]): Future[Map[V,Map[K,delta.Tick]]] = Future {
    snapshots
      .flatMap {
        case (key, snapshot) => extract(refName, snapshot.state).map(v => (v, key, snapshot.tick))
      }
      .groupBy(_._1)
      .view
        .filter(_._2.size > 1)
        .mapValues(_.map(t => t._2 -> t._3).toMap)
      .toMap
  }

  type QueryValue = Any
  protected def queryMatch(name: String, matchValue: QueryValue, state: S): Boolean
  protected def querySnapshot(
      nameValue: (String, QueryValue), more: (String, QueryValue)*)
      : Future[Map[K, Snapshot]] = Future {

    val filter = nameValue :: more.toList

    snapshots
      .filter {
        case (_, Snapshot(state, _, _)) =>
          filter.forall {
            case (name, expected) => queryMatch(name, expected, state)
          }
      }.toMap

    }

  protected def queryTick(
      nameValue: (String, QueryValue), more: (String, QueryValue)*)
      : Future[Map[K, Long]] =
    querySnapshot(nameValue, more: _*)
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
