package ulysses.ddd

import scala.reflect.ClassTag

/**
  * Aggregate root entity definition.
  */
trait AggregateRoot /* [Entity <: AnyRef, Event, State <: AnyRef, Channel] */ {
  type Channel
  type Entity
  type Event
  type State <: AnyRef

  def channel: Channel

  /**
   * Instantiate new mutator, optionally with state.
   * @param state Optional state
   */
  def newMutator(state: Option[State]): StateMutator[Event, State]

  /**
   * Get the mutator used for the entity instance.
   * @param entity The instance to get mutator from
   */
  def getMutator(entity: Entity): StateMutator[Event, State]

  /**
   * Initialize entity.
   * @param state The internal state
   * @param mergeEvents Any potential events to merge
   * @return The Entity instance
   */
  def init(state: State, mergeEvents: Vector[Event]): Entity

  /**
   * Convenience method for ensuring invariants
   * are not violated.
   */
  def checkInvariants(state: State): Unit
}
