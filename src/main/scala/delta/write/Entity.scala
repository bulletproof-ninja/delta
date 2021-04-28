package delta.write

import delta._

/**
  * Type-class for Entity definition.
  * Only for top-level (aggregate root) entities.
  * @tparam S Entity's internal state representation. Must be an immutable type (typically a case class)
  * @tparam EVT Entity event type
  * @param name Entity name. This is used as `channel` in the event store
  * @param projector Entity state projector
  */
abstract class Entity[S >: Null, EVT](name: String, projector: Projector[S, EVT]) {

  /** Entity id type. */
  type Id

  /**
    * The entity type. This is the type that handles commands and
    * verifies domain rules, and emits events when the commands are
    * in compliance. Events are applied to the internal state, through
    * the supplied projector, and immediately visible.
    */
  type Type

  /**
    * Initialize entity instance.
    * @param state The internal state. This is always up-to-date, regardless of any concurrent updates
    * @param concurrentUpdates Any concurrent transactions, i.e. that are unknown to the updater. Only
    * supplied if `expectedRevision` is provided when calling one of the `delta.write.Repository#update` methods.
    * @return The Entity instance
    */
  protected def init(id: Id, stateRef: StateRef, concurrentUpdates: List[Transaction]): Type

  /**
    * Get state used by the entity instance.
    * @param instance The instance to get mutator from
    */
  protected def StateRef(entity: Type): StateRef

  /**
    * Validate invariants. Convenience method
    * for unifying invariant validation to a single place.
    * @note This is only called before persisting, so if
    * multiple commands are applied, it may not be clear
    * which one caused the violation. In such cases,
    * validate manually.
    */
  protected def validate(data: S): Unit

  type StateRef = delta.write.StateRef[S, EVT]

  protected type Transaction = delta.Transaction[_, EVT]

  /** Stream channel. Matches entity name. */
  val channel: Channel = Channel(name)

  def newStateRef(initData: S = null): StateRef = new StateRef(projector, initData)

  private[write] def validatedState(entity: Type): StateRef = {
    val s = StateRef(entity)
    validate(s.get)
    s
  }

  private[write] def initEntity(
      id: Id,
      state: S,
      concurrentUpdates: List[Transaction]): Type =
    init(id, newStateRef(state), concurrentUpdates)

}
