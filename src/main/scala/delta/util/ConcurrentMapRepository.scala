package delta.util

import scala.collection.concurrent.{ Map => CMap, TrieMap }
import scala.concurrent.{ ExecutionContext, Future }

import delta.ddd.{ DuplicateIdException, Repository, Revision, UnknownIdException }
import scuff.concurrent.{ ScuffScalaFuture, Threads }
import delta.ddd.ImmutableState

/**
  * Repository backed by concurrent map.
  * Mostly useful for testing.
  */
class ConcurrentMapRepository[K, V <: AnyRef](
    map: CMap[K, (V, Int)] = new TrieMap[K, (V, Int)])(implicit ec: ExecutionContext = Threads.Blocking)
  extends Repository[K, V] with ImmutableState[V] {

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

  private def tryUpdate[R](id: K, expectedRevision: Revision, metadata: Map[String, String], updateThunk: (V, Int) => Future[V]): Future[Int] = {
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
      expectedRevision: Revision, id: K,
      metadata: Map[String, String], updateThunk: (V, Int) => Future[V]): Future[Int] =
    Future(tryUpdate(id, expectedRevision, metadata, updateThunk)).flatten

}
