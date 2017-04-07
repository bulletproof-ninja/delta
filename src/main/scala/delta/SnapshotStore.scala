package delta

import collection.Map
import scala.concurrent.Future

trait SnapshotStore[K, S] {
  def maxTick: Future[Option[Long]]
  def read(key: K): Future[Option[Snapshot[S]]]
  def write(key: K, snapshot: Snapshot[S]): Future[Unit]
  def readBatch(keys: Iterable[K]): Future[Map[K, Snapshot[S]]]
  def writeBatch(batch: Map[K, Snapshot[S]]): Future[Unit]
  def refresh(key: K, revision: Int, tick: Long): Future[Unit]
  def refreshBatch(revisions: Map[K, (Int, Long)]): Future[Unit]
}

private[delta] object SnapshotStore {
  val UnitFuture = Future.successful(())
  val NoneFuture = Future successful None
  private[this] val emptyMapFuture = Future successful collection.immutable.Map.empty[Any, AnyRef]
  def EmptyMapFuture[K, V] = emptyMapFuture.asInstanceOf[Future[Map[K, V]]]
  private[this] val Empty = new SnapshotStore[Any, AnyRef] {
    def maxTick: Future[Option[Long]] = NoneFuture
    def read(id: Any): Future[Option[Snapshot[AnyRef]]] = NoneFuture
    def write(id: Any, snapshot: Snapshot[AnyRef]): Future[Unit] = UnitFuture
    def readBatch(keys: Iterable[Any]): Future[Map[Any, Snapshot[AnyRef]]] = EmptyMapFuture
    def writeBatch(snapshots: Map[Any, Snapshot[AnyRef]]): Future[Unit] = UnitFuture
    def refresh(key: Any, revision: Int, tick: Long): Future[Unit] = UnitFuture
    def refreshBatch(revisions: Map[Any, (Int, Long)]): Future[Unit] = UnitFuture
  }

  def empty[K, T] = Empty.asInstanceOf[SnapshotStore[K, T]]
}
