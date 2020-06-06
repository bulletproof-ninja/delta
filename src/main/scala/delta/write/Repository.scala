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
   * @param metadata Optional metadata.
   * @return The id and tick if successful,
   * or [[delta.write.DuplicateIdException]] if id already exists and id is constant
   */
  def insert(
      newId: => ID,
      entity: Entity,
      causalTick: Tick = Long.MinValue)(
      implicit
      metadata: Metadata): Future[ID]
}

sealed trait Updates[ID, E] {
  repo: Repository[ID, E] =>

  protected type Loaded
  type Entity = E

  type UT[_]
  type UM[_]

  protected def revision(loaded: Loaded): Revision

  protected def update[R](
      updateThunk: Loaded => Future[UT[R]],
      id: ID, causalTick: Long,
      expectedRevision: Option[Revision])(
      implicit
      metadata: Metadata): Future[UM[R]]

  /**
   * Update entity.
   * @note The `updateThunk` should be side-effect free, as it
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
      id: ID,
      expectedRevision: Option[Revision] = None,
      causalTick: Tick = Long.MinValue)(
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
    update(checkedUpdate, id, causalTick, expectedRevision)

  }

  /**
   * Update entity.
   * @note The `updateThunk` should be side-effect free, as it
   * may be invoked multiple times, if there are concurrent
   * updates.
   * @param id The entity id
   * @param metadata Metadata.
   * @param updateThunk The code block responsible for updating.
   * Will receive the instance and current revision.
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
   * @param expectedRevision The expected revision
   * @param updateThunk The code block responsible for updating.
   * Will receive the instance and current revision.
   * @param metadata Metadata.
   * @return New revision, or [[delta.write.UnknownIdException]] if unknown id.
   */
  final def update[R](id: ID, expectedRevision: Revision)(
      updateThunk: Loaded => Future[UT[R]])(
      implicit
      metadata: Metadata): Future[UM[R]] =
    update(id, Some(expectedRevision))(updateThunk)

  /**
   * Update entity.
   * @note The `updateThunk` should be side-effect free, as it
   * may be invoked multiple times, if there are concurrent
   * updates.
   * @param id The entity id
   * @param expectedRevision The expected revision
   * @param causalTick The tick of the entity causing this update
   * @param updateThunk The code block responsible for updating.
   * Will receive the instance and current revision.
   * @param metadata Metadata.
   * @return New revision, or [[delta.write.UnknownIdException]] if unknown id.
   */
  final def update[R](id: ID, expectedRevision: Revision, causalTick: Tick)(
      updateThunk: Loaded => Future[UT[R]])(
      implicit
      metadata: Metadata): Future[UM[R]] =
    update(id, Some(expectedRevision), causalTick)(updateThunk)

  /**
   * Update entity.
   * @note The `updateThunk` should be side-effect free, as it
   * may be invoked multiple times, if there are concurrent
   * updates.
   * @param id The entity id
   * @param causalTick The tick of the entity causing this update
   * @param updateThunk The code block responsible for updating.
   * Will receive the instance and current revision.
   * @param metadata Metadata.
   * @return New revision, or [[delta.write.UnknownIdException]] if unknown id.
   */
  final def update[R](id: ID, causalTick: Tick)(
      updateThunk: Loaded => Future[UT[R]])(
      implicit
      metadata: Metadata): Future[UM[R]] =
    update(id, None, causalTick)(updateThunk)

}

trait MutableEntity {
  repo: Repository[_, _] =>

  protected type Loaded = (Entity, Revision)

  type UT[R] = R
  type UM[R] = (R, Revision)

}

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
