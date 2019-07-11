package delta

import scala.concurrent.Future

trait SnapshotStore[K, T] {
  type Snapshot = delta.Snapshot[T]
  def read(key: K): Future[Option[delta.Snapshot[T]]]
  def write(key: K, snapshot: Snapshot): Future[Unit]
}

object SnapshotStore {
  private[this] val UnitFuture = Future.successful(())
  private[this] val NoneFuture = Future successful None
  private[this] val Empty = new SnapshotStore[Any, AnyRef] {
    def read(key: Any): Future[Option[Snapshot]] = NoneFuture
    def write(key: Any, snapshot: Snapshot): Future[Unit] = UnitFuture
  }
  def empty[K, T] = Empty.asInstanceOf[SnapshotStore[K, T]]
}
