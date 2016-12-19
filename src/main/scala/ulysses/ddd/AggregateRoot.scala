package ulysses.ddd

/**
  * Type-class for aggregate root entity.
  */
trait AggregateRoot {

  type Id
  type Entity
  type Event
  type State <: AnyRef
  type Channel

  def channel: Channel

  /**
    * Instantiate new mutator, with existing state or from scratch.
    * @param state Optional state
    */
  def newMutator(state: Option[State]): StateMutator[Event, State]

  /**
    * Initialize entity.
    * @param state The internal state
    * @param mergeEvents Any potential events to merge
    * @return The Entity instance
    */
  def init(state: State, mergeEvents: List[Event]): Entity

  /**
    * Get the mutator used for the entity instance.
    * @param entity The instance to get mutator from
    */
  def done(entity: Entity): StateMutator[Event, State]

  /**
    * Convenience method for ensuring invariants
    * are not violated. Can be noop, if not needed.
    */
  def checkInvariants(state: State): Unit
}
