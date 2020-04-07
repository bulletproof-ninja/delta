package delta.write

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
   * @return The latest revision of entity or [[delta.write.UnknownIdException]]
   */
  def load(id: ID): Future[(E, Int)]

  /**
   * Insert new entity. Will, by definition, always be given revision `0`.
   * @param newId The new instance id function
   * @param entity The instance to insert
   * @param metadata Optional metadata.
   * @return The id if successful,
   * or [[delta.write.DuplicateIdException]] if id already exists and id is constant
   */
  def insert(newId: => ID, entity: E)(implicit metadata: Metadata): Future[ID]
}

sealed trait Updates[ID, E] {
  type UT[_]
  type UM[_]

  protected def update[R](
      expectedRevision: Option[Int], id: ID,
      updateThunk: (E, Int) => Future[UT[R]])(
      implicit
      metadata: Metadata): Future[UM[R]]

  /**
   * Update entity.
   * NOTE: The `updateThunk` should be side-effect free, as it
   * may be invoked multiple times, if there are concurrent
   * updates.
   * @param id The entity id
   * @param expectedRevision The revision that is expected to be updated.
   * @param metadata Optional metadata.
   * @param updateThunk The code block responsible for updating.
   * Will receive the instance and revision.
   * @return New revision, or [[delta.write.UnknownIdException]] if unknown id.
   */
  final def update[R](
      id: ID, expectedRevision: Option[Int] = None)(
      updateThunk: (E, Int) => Future[UT[R]])(
      implicit
      metadata: Metadata): Future[UM[R]] = {

    val proxy = (entity: E, revision: Int) => {
      if ((expectedRevision getOrElse 0) > revision) {
        throw new IllegalStateException(
          s"Expected revision ${expectedRevision.get} is higher than actual revision of $revision, for $id")
      }
      updateThunk(entity, revision)
    }

    update(expectedRevision, id, proxy)

  }

  /**
   * Update entity.
   * NOTE: The `updateThunk` should be side-effect free, as it
   * may be invoked multiple times, if there are concurrent
   * updates.
   * @param id The entity id
   * @param metadata Metadata.
   * @param updateThunk The code block responsible for updating.
   * Will receive the instance and current revision.
   * @return New revision, or [[delta.write.UnknownIdException]] if unknown id.
   */
  final def update[R](id: ID)(
      updateThunk: (E, Int) => Future[UT[R]])(
      implicit
      metadata: Metadata): Future[UM[R]] =
    update(id, None)(updateThunk)

}

trait MutableEntity {
  repo: Repository[_, _] =>

  type UT[R] = R
  type UM[R] = (R, Int)

}

trait ImmutableEntity[E] {
  repo: Repository[_, E] =>

  type UT[_] = E
  type UM[_] = Int

}

final case class UnknownIdException(id: Any) extends RuntimeException(s"Unknown id: $id")
final case class DuplicateIdException(id: Any, metadata: Map[String, String])
  extends RuntimeException(s"Id already exists: $id${DuplicateIdException.errorMessageSuffix(metadata)}")
private object DuplicateIdException {
  def errorMessageSuffix(metadata: Map[String, String]): String =
    if (metadata.isEmpty) "" else s", failed transaction metadata: ${metadata.mkString(", ")}"
}
