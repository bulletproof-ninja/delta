package delta.util

import scala.collection.concurrent.{ Map => CMap, TrieMap }
import scala.concurrent.{ ExecutionContext, Future }
import scuff.concurrent.{ ScuffScalaFuture, Threads }
import delta.ddd.DuplicateIdException
import delta.ddd.Repository
import delta.ddd.UnknownIdException

/**
  * Repository backed by concurrent map.
  * Mostly useful for testing.
  */
class MapRepository[K, V <: AnyRef](
  map: CMap[K, (V, Int)] = new TrieMap[K, (V, Int)])(implicit ec: ExecutionContext = Threads.Blocking)
    extends Repository[K, V] {

  def insert(id: K, entity: V, metadata: Map[String, String]): Future[Int] = Future {
    map.putIfAbsent(id, entity -> 0) match {
      case None => 0
      case _ => throw new DuplicateIdException(id)
    }
  }

  def exists(id: K): Future[Option[Int]] = Future(map.get(id).map(_._2))

  def load(id: K): Future[(V, Int)] = Future {
    map.get(id) match {
      case None => throw new UnknownIdException(id)
      case Some(found) => found
    }
  }

  private def tryUpdate(id: K, expectedRevision: Option[Int], metadata: Map[String, String], updateThunk: (V, Int) => Future[V]): Future[Int] = {
    map.get(id) match {
      case None => Future failed new UnknownIdException(id)
      case Some(oldE @ (ar, rev)) =>
        updateThunk(ar, rev).flatMap { ar =>
          val newRev = rev + 1
          if (map.replace(id, oldE, ar -> newRev)) {
            Future successful newRev
          } else {
            tryUpdate(id, expectedRevision, metadata, updateThunk)
          }
        }
    }
  }

  def update(
    id: K, expectedRevision: Option[Int],
    metadata: Map[String, String], updateThunk: (V, Int) => Future[V]): Future[Int] =
    Future(tryUpdate(id, expectedRevision, metadata, updateThunk)).flatten

}
