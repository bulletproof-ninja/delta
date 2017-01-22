package delta

import collection.Map
import scala.concurrent.Future

trait SnapshotStore[K, T >: Null] {
  def get(key: K): Future[Option[Snapshot[T]]]
  def set(key: K, snapshot: Snapshot[T]): Future[Unit]
  def getAll(keys: Iterable[K]): Future[Map[K, Snapshot[T]]]
  def setAll(snapshots: Map[K, Snapshot[T]]): Future[Unit]
  def update(key: K, revision: Int, tick: Long): Future[Unit]
  def updateAll(revisions: Map[K, (Int, Long)]): Future[Unit]
}

private[delta] object SnapshotStore {
  val NoneFuture = Future.successful(None)
  val EmptyMapFuture = Future.successful(Map.empty[Any, Snapshot[AnyRef]])
  val UnitFuture = Future.successful(())
  private[this] val Empty = new SnapshotStore[Any, AnyRef] {
    def get(id: Any): Future[Option[Snapshot[AnyRef]]] = NoneFuture
    def set(id: Any, snapshot: Snapshot[AnyRef]): Future[Unit] = UnitFuture
    def getAll(keys: Iterable[Any]): Future[Map[Any, Snapshot[AnyRef]]] = EmptyMapFuture
    def setAll(snapshots: Map[Any, Snapshot[AnyRef]]): Future[Unit] = UnitFuture
    def update(key: Any, revision: Int, tick: Long): Future[Unit] = UnitFuture
    def updateAll(revisions: Map[Any, (Int, Long)]): Future[Unit] = UnitFuture
  }

  def empty[K, T >: Null] = Empty.asInstanceOf[SnapshotStore[K, T]]
}
