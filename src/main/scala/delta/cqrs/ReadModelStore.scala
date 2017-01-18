package delta.cqrs

import collection.Map
import scala.concurrent.Future

trait ReadModelStore[K, D] {
  def lastTick(): Future[Option[Long]]
  def get(key: K): Future[Option[ReadModel[D]]]
  def getAll(keys: Iterable[K]): Future[Map[K, ReadModel[D]]]
  def set(key: K, data: ReadModel[D]): Future[Unit]
  def setAll(map: Map[K, ReadModel[D]]): Future[Unit]
  def update(key: K, revision: Int, tick: Long): Future[Unit]
  def updateAll(revisions: Map[K, (Int, Long)]): Future[Unit]
}
