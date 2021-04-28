package delta.util

import scala.collection.concurrent.{ Map => CMap, TrieMap }
import scala.concurrent.{ ExecutionContext, Future }

import delta._
import delta.write.{ DuplicateIdException, Repository, Metadata }
import delta.write.ImmutableEntity

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

  def insert(id: => K, entity: E)(implicit metadata: Metadata): Future[K] = Future {
    insertImpl(id, id, entity, metadata.toMap)
  }

  @annotation.tailrec
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
      id: K, expectedRevision: Option[Revision],
      // metadata: Map[String, String],
      updateThunk: Loaded => Future[(E, Metadata)]): Future[(Revision, Metadata)] =
    map.get(id) match {
      case None => Future failed new UnknownIdException(id)
      case Some(oldE) =>
        updateThunk(oldE).flatMap {
          case (entitiy, metadata) =>
          val newRev = oldE._2 + 1
          if (map.replace(id, oldE, entitiy -> newRev)) {
            Future successful newRev -> metadata
          } else {
            tryUpdate(id, expectedRevision, /*metadata,*/ updateThunk)
          }
        }
    }

  protected def update(
      updateThunk: Loaded => Future[UpdateReturn],
      id: K, expectedRevision: Option[Revision])
      // (
      // implicit
      // metadata: Metadata)
      : Future[Revision] =
    Future(tryUpdate(id, expectedRevision, /*metadata.toMap,*/ updateThunk))
      .flatMap { _.map(_._1) }

}
