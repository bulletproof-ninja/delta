package delta.write

import scala.concurrent.Future

import delta.{ Revision, Tick }

/**
 * Entity repository.
 */
trait Repository[ID, E]
extends Updates[ID, E] {

  /**
   * Get current revision, if exists.
   * @return Current revision or `None` if unknown id.
   */
  def exists(id: ID): Future[Option[Revision]]

  /**
   * Load entity. Only for reading.
   * Modifications cannot be saved.
   * @param id The instance ID
   * @return The latest revision of entity or [[delta.write.UnknownIdException]]
   */
  def load(id: ID): Future[Loaded]

  /**
   * Insert new entity. Will, by definition, always be given revision `0`.
   * @param newId The new instance id function
   * @param entity The instance to insert
   * @param metadata Implicit metadata.
   * @return The new id if successful,
   * or [[delta.write.DuplicateIdException]] if id already exists (and id function is constant)
   */
  def insert(
      newId: => ID,
      entity: Entity)(
      implicit
      metadata: Metadata): Future[ID]
}

/** Repository update methods. */
sealed trait Updates[ID, E] {
  repo: Repository[ID, E] =>

  type Loaded
  type Entity = E

  type UT[_]
  type UM[_]

  protected def revision(loaded: Loaded): Revision

  protected def update[R](
      updateThunk: Loaded => Future[UT[R]],
      id: ID, expectedRevision: Option[Revision])(
      implicit
      metadata: Metadata): Future[UM[R]]

  /**
   * Update entity.
   * @note The `updateThunk` should be side-effect free, as it
   * may be invoked multiple times, if there are concurrent
   * updates.
   * @param id The entity id
   * @param expectedRevision Optional revision that is expected to be updated.
   * @param updateThunk The code block responsible for updating.
   * Will receive the instance and revision.
   * @param metadata Implicit metadata.
   * @return New revision, or [[delta.write.UnknownIdException]] if unknown id.
   */
  final def update[R](
      id: ID,
      expectedRevision: Option[Revision])(
      updateThunk: Loaded => Future[UT[R]])(
      implicit
      metadata: Metadata): Future[UM[R]] = {

    val checkedUpdate = (loaded: Loaded) => {
      if ((expectedRevision getOrElse 0) > revision(loaded)) {
        throw new IllegalStateException(
          s"Expected revision ${expectedRevision.get} is higher than current revision of ${revision(loaded)}, for $id")
      }
      updateThunk(loaded)
    }

    // Delegate:
    update(checkedUpdate, id, expectedRevision)

  }

  /**
   * Update entity.
   * @note The `updateThunk` should be side-effect free, as it
   * may be invoked multiple times, if there are concurrent
   * updates.
   * @param id The entity id
   * @param updateThunk The code block responsible for updating.
   * @param metadata Implicit metadata.
   * @return New revision, or [[delta.write.UnknownIdException]] if unknown id.
   */
  final def update[R](id: ID)(
      updateThunk: Loaded => Future[UT[R]])(
      implicit
      metadata: Metadata): Future[UM[R]] =
    update(id, None)(updateThunk)

  /**
   * Update entity.
   * @note The `updateThunk` should be side-effect free, as it
   * may be invoked multiple times, if there are concurrent
   * updates.
   * @param id The entity id
   * @param expectedRevision The revision that is expected to be updated.
   * @param updateThunk The code block responsible for updating.
   * @param metadata Implicit metadata.
   * @return New revision, or [[delta.write.UnknownIdException]] if unknown id.
   */
  final def update[R](id: ID, expectedRevision: Revision)(
      updateThunk: Loaded => Future[UT[R]])(
      implicit
      metadata: Metadata): Future[UM[R]] =
    update(id, Some(expectedRevision))(updateThunk)

}

/** Repository trait for mutable entities. */
trait MutableEntity {
  repo: Repository[_, _] =>

  type Loaded = (Entity, Revision)

  type UT[R] = R
  type UM[R] = (R, Revision)

}

/** Repository trait for immutable entities. */
trait ImmutableEntity {
  repo: Repository[_, _] =>

  type UT[_] = Entity
  type UM[_] = Revision

}

final case class UnknownIdException(id: Any) extends RuntimeException(s"Unknown id: $id")
final case class DuplicateIdException(id: Any, metadata: Map[String, String])
  extends RuntimeException(s"Id already exists: $id${DuplicateIdException.errorMessageSuffix(metadata)}")
private object DuplicateIdException {
  def errorMessageSuffix(metadata: Map[String, String]): String =
    if (metadata.isEmpty) "" else s", failed transaction metadata: ${metadata.mkString(", ")}"
}
