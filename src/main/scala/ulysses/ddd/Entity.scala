package ulysses.ddd

/**
  * Type-class for entity.
  */
trait Entity {

  type Type
  type Id
  type Event
  type State <: AnyRef

  /**
    * Instantiate new mutator, with existing state or from scratch.
    * @param state Optional state
    */
  def newMutator(state: Option[State]): StateMutator[Event, State]

  /**
    * Initialize entity instance.
    * @param state The internal state
    * @param mergeEvents Any potential events to merge
    * @return The Entity instance
    */
  def init(state: State, mergeEvents: List[Event]): Type

  /**
    * Get the mutator used for the entity instance.
    * @param instance The instance to get mutator from
    */
  def done(instance: Type): StateMutator[Event, State]

  /**
    * Convenience method for ensuring invariants
    * are not violated. Can be noop, if not needed.
    */
  def checkInvariants(state: State): Unit
}
