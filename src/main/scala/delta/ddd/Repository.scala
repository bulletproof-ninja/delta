package delta.ddd

import scala.collection.immutable.Map
import scala.concurrent.Future
import language.higherKinds

/**
  * Entity repository.
  */
trait Repository[ID, E] extends Updates[ID, E] {

  /**
    * Get current revision, if exists.
    * @return Current revision or `None` if unknown id.
    */
  def exists(id: ID): Future[Option[Int]]

  /**
    * Load entity. Only for reading.
    * Modifications cannot be saved.
    * @param id The instance ID
    * @return The latest revision of entity or [[delta.ddd.UnknownIdException]]
    */
  def load(id: ID): Future[(E, Int)]

  /**
    * Insert new entity. Will, by definition, be given revision `0`.
    * @param id The instance id
    * @param entity The instance to insert
    * @param metadata Optional metadata.
    * @return The revision (always `0`) if successful,
    * or [[delta.ddd.DuplicateIdException]] if id already exists
    */
  def insert(id: ID, entity: E, metadata: Map[String, String] = Map.empty): Future[Int]
}

sealed trait Updates[ID, S] {
  type UT[_]
  type UM[_]

  protected def update[R](
      expectedRevision: Revision, id: ID,
      metadata:    Map[String, String],
      updateThunk: (S, Int) => Future[UT[R]]): Future[UM[R]]

  /**
    * Update entity.
    * @param id The instance id
    * @param expectedRevision The revision that is expected to be updated.
    * @param metadata Optional metadata.
    * @param updateThunk The code block responsible for updating.
    * Will receive the instance and revision.
    * @return New revision, or [[delta.ddd.UnknownIdException]] if unknown id.
    */
  final def update[R](
      id: ID, expectedRevision: Revision = Revision.Latest, metadata: Map[String, String] = Map.empty)(
      updateThunk: (S, Int) => Future[UT[R]]): Future[UM[R]] = {
    val proxy = (state: S, revision: Int) => {
      expectedRevision.validate(revision)
      updateThunk(state, revision)
    }
    update(expectedRevision, id, metadata, proxy)
  }

  /**
    * Update entity.
    * @param id The instance id
    * @param metadata Metadata.
    * @param updateThunk The code block responsible for updating.
    * Will receive the instance and current revision.
    * @return New revision, or [[delta.ddd.UnknownIdException]] if unknown id.
    */
  final def update[R](
      id: ID, metadata: Map[String, String])(
      updateThunk: (S, Int) => Future[UT[R]]): Future[UM[R]] =
    update(id, Revision.Latest, metadata)(updateThunk)

}

trait MutableState {
  repo: Repository[_, _] =>

  type UT[R] = R
  type UM[R] = (R, Int)

}

trait ImmutableState[S] {
  repo: Repository[_, S] =>

  type UT[_] = S
  type UM[_] = Int

}

final case class UnknownIdException(id: Any) extends RuntimeException(s"Unknown id: $id")
final case class DuplicateIdException(id: Any) extends RuntimeException(s"Id already exists: $id")
