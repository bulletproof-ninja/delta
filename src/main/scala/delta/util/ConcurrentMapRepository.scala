package delta.util

import scala.collection.concurrent.{ Map => CMap, TrieMap }
import scala.concurrent.{ ExecutionContext, Future }

import delta._
import delta.write.{ DuplicateIdException, Repository, UnknownIdException }
import delta.write.ImmutableEntity
import delta.write.Metadata

/**
 * Repository backed by concurrent map.
 * Mostly useful for testing.
 */
class ConcurrentMapRepository[K, E <: AnyRef](
  map: CMap[K, (E, Revision)] = new TrieMap[K, (E, Revision)])(
  implicit
  ec: ExecutionContext)
extends Repository[K, E]
with ImmutableEntity {

  type Loaded = (E, Revision)
  protected def revision(loaded: Loaded) = loaded._2

  def insert(id: => K, entity: E, causalTick: Tick)(
      implicit
      metadata: Metadata): Future[K] = Future {
    insertImpl(id, id, entity, metadata.toMap)
  }

  private def insertImpl(
      id: K, generateId: => K, entity: E,
      metadata: Map[String, String]): K = {
    map.putIfAbsent(id, entity -> 0) match {
      case None => id
      case _ =>
        val newId = generateId
        if (newId == id) throw new DuplicateIdException(id, metadata)
        else insertImpl(newId, generateId, entity, metadata)
    }
  }

  def exists(id: K): Future[Option[Revision]] = Future(map.get(id).map(_._2))

  def load(id: K): Future[Loaded] = Future {
    map.get(id) match {
      case None => throw new UnknownIdException(id)
      case Some(found) => found
    }
  }

  private def tryUpdate[R](
      id: K, expectedRevision: Option[Revision], metadata: Map[String, String],
      updateThunk: Loaded => Future[E]): Future[Revision] = {
    map.get(id) match {
      case None => Future failed new UnknownIdException(id)
      case Some(oldE) =>
        updateThunk(oldE).flatMap { ar =>
          val newRev = oldE._2 + 1
          if (map.replace(id, oldE, ar -> newRev)) {
            Future successful newRev
          } else {
            tryUpdate(id, expectedRevision, metadata, updateThunk)
          }
        }
    }
  }

  protected def update[R](
      updateThunk: Loaded => Future[UT[R]],
      id: K, causalTick: Tick,
      expectedRevision: Option[Revision])(
      implicit
      metadata: Metadata): Future[UM[R]] =
    Future(tryUpdate(id, expectedRevision, metadata.toMap, updateThunk)).flatMap(identity)

}
