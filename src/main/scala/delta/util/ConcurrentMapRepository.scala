package delta.util

import scala.collection.concurrent.{ Map => CMap, TrieMap }
import scala.concurrent.{ ExecutionContext, Future }

import delta.ddd.{ DuplicateIdException, Repository, UnknownIdException }
import scuff.concurrent.{ Threads }
import delta.ddd.ImmutableEntity

/**
  * Repository backed by concurrent map.
  * Mostly useful for testing.
  */
class ConcurrentMapRepository[K, V <: AnyRef](
    map: CMap[K, (V, Int)] = new TrieMap[K, (V, Int)])(implicit ec: ExecutionContext = Threads.Blocking)
  extends Repository[K, V] with ImmutableEntity[V] {

  def insert(id: K, entity: V, metadata: Map[String, String]): Future[K] = Future {
    map.putIfAbsent(id, entity -> 0) match {
      case None => id
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

  private def tryUpdate[R](id: K, expectedRevision: Option[Int], metadata: Map[String, String], updateThunk: (V, Int) => Future[V]): Future[Int] = {
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

  def update[_](
      expectedRevision: Option[Int], id: K,
      metadata: Map[String, String], updateThunk: (V, Int) => Future[V]): Future[Int] =
    Future(tryUpdate(id, expectedRevision, metadata, updateThunk)).flatMap(identity)

}
