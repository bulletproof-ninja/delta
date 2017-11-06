package delta

import scala.concurrent.Future

trait SnapshotStore[K, S] {
  type Snapshot = delta.Snapshot[S]
  def read(key: K): Future[Option[Snapshot]]
  def write(key: K, snapshot: Snapshot): Future[Unit]
}
private[delta] object SnapshotStore {
  private[this] val UnitFuture = Future.successful(())
  private[this] val NoneFuture = Future successful None
  private[this] val Empty = new SnapshotStore[Any, AnyRef] {
    def read(key: Any): Future[Option[Snapshot]] = NoneFuture
    def write(key: Any, snapshot: Snapshot): Future[Unit] = UnitFuture
  }
  def empty[K, T] = Empty.asInstanceOf[SnapshotStore[K, T]]
}
