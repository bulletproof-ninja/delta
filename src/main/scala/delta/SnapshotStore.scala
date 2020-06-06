package delta

import scala.concurrent.Future
import scuff.concurrent._

trait SnapshotReader[K, S] {

  type Snapshot = delta.Snapshot[S]
  def read(key: K): Future[Option[Snapshot]]

  def asSnapshotReader[R](implicit adaptState: S => R): SnapshotReader[K, R] =
    asSnapshotReader[K, R]

  def asSnapshotReader[ID, R](
      implicit
      keyConv: ID => K,
      stateConv: S => R): SnapshotReader[ID, R] =
    new SnapshotReader[ID, R] {
      def read(id: ID): Future[Option[delta.Snapshot[R]]] =
        SnapshotReader.this.read(id: K)
          .map {
            _.map {
              _.map(stateConv)
            }
          }(Threads.PiggyBack)
    }


}

trait SnapshotStore[K, S]
extends SnapshotReader[K, S] {
  def write(key: K, snapshot: Snapshot): Future[Unit]
  final def write(entry: (K, Snapshot)): Future[Unit] = write(entry._1, entry._2)
}

object SnapshotStore {
  private[this] val Empty = new SnapshotStore[Any, AnyRef] {
    def read(key: Any): Future[Option[Snapshot]] = Future.none
    def write(key: Any, snapshot: Snapshot): Future[Unit] = Future.unit
  }
  def empty[K, T] = Empty.asInstanceOf[SnapshotStore[K, T]]
}
