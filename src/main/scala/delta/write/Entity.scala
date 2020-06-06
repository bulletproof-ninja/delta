package delta.write

import delta._

/**
  * Type-class for Entity definition.
  * Only for top-level (aggregate root) entities.
  */
abstract class Entity[S >: Null, EVT](name: String, projector: Projector[S, EVT]) {

  val channel: Channel = Channel(name)

  type Id
  type Type

  type State = delta.write.State[S, EVT]

  protected type Transaction = delta.Transaction[_, EVT]

  def newState(initState: S = null): State = new State(projector, initState)

  private[write] def validatedState(entity: Type): State = {
    val s = state(entity)
    validate(s.get)
    s
  }

  private[write] def initEntity(
      state: S,
      concurrentUpdates: List[Transaction]): Type =
    init(newState(state), concurrentUpdates)

  /**
    * Initialize entity instance.
    * @param state The internal state. This is always up-to-date, regardless of any concurrent updates
    * @param concurrentUpdates Any concurrent transactions, i.e. that are unknown to the updater.
    * @return The Entity instance
    */
  protected def init(state: State, concurrentUpdates: List[Transaction]): Type

  /**
    * Get state used by the entity instance.
    * @param instance The instance to get mutator from
    */
  protected def state(entity: Type): State

  /**
    * Validate invariants. Convenience method
    * for unifying invariant validation to
    * a single place. Ideally, checks should happen
    * at every state transition, but this is not
    * always convenient.
    */
  protected def validate(state: S): Unit

}
