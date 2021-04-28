package delta

import scala.concurrent._
import scuff.concurrent._
import delta.util.Logging

trait SnapshotReader[K, S] {

  type Snapshot = delta.Snapshot[S]

  protected def logging: Logging

  def read(key: K): Future[Option[Snapshot]]

  def asSnapshotReader[R](
      pf: PartialFunction[(K, S), R])
      : SnapshotReader[K, R] = {

    val reader = SnapshotReader.this

    new SnapshotReader[K, R] {
      protected def logging: Logging = reader.logging
      def read(key: K): Future[Option[delta.Snapshot[R]]] =
        reader.read(key)
          .map {
            _.flatMap { snapshot =>
              val t = key -> snapshot.state
              if (pf isDefinedAt t) Some(snapshot.copy(state = pf(t)))
              else None
            }
          }(Threads.PiggyBack)

    }

  }

  def asSnapshotReader[ID, R](
      implicit
      keyConv: ID => K,
      stateConv: S => R): SnapshotReader[ID, R] = {

    val reader = SnapshotReader.this

    new SnapshotReader[ID, R] {
      protected def logging: Logging = reader.logging
      def read(id: ID): Future[Option[delta.Snapshot[R]]] =
        reader.read(id: K)
          .map {
            _.map {
              _.map(stateConv)
            }
          }(Threads.PiggyBack)
    }
  }

}

trait SnapshotStore[K, S]
extends SnapshotReader[K, S] {
  def write(key: K, snapshot: Snapshot): Future[Unit]
  final def write(entry: (K, Snapshot)): Future[Unit] = write(entry._1, entry._2)
}

object SnapshotStore {
  private[this] val Empty = new SnapshotStore[Any, AnyRef] {
    protected def logging = Logging.NoOp
    def read(key: Any): Future[Option[Snapshot]] = Future.none
    def write(key: Any, snapshot: Snapshot): Future[Unit] = Future.unit
  }
  def empty[K, T] = Empty.asInstanceOf[SnapshotStore[K, T]]
}
