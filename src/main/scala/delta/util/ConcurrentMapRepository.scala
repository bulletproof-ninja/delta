package delta.util

import scala.collection.concurrent.{ Map => CMap, TrieMap }
import scala.concurrent.{ ExecutionContext, Future }

import delta.write.{ DuplicateIdException, Repository, UnknownIdException }
import delta.write.ImmutableEntity
import delta.write.Metadata

/**
 * Repository backed by concurrent map.
 * Mostly useful for testing.
 */
class ConcurrentMapRepository[K, V <: AnyRef](
    exeCtx: ExecutionContext,
    map: CMap[K, (V, Int)] = new TrieMap[K, (V, Int)])
  extends Repository[K, V] with ImmutableEntity[V] {

  private implicit def ec = exeCtx

  def insert(id: => K, entity: V)(
      implicit
      metadata: Metadata): Future[K] = Future {
    insertImpl(id, id, entity, metadata.toMap)
  }

  private def insertImpl(id: K, generateId: => K, entity: V, metadata: Map[String, String]): K = {
    map.putIfAbsent(id, entity -> 0) match {
      case None => id
      case _ =>
        val newId = generateId
        if (newId == id) throw new DuplicateIdException(id, metadata)
        else insertImpl(newId, generateId, entity, metadata)
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

  protected def update[_](
      expectedRevision: Option[Int], id: K,
      updateThunk: (V, Int) => Future[V])(
      implicit
      metadata: Metadata): Future[Int] =
    Future(tryUpdate(id, expectedRevision, metadata.toMap, updateThunk)).flatMap(identity)

}
