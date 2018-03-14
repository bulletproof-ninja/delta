package delta.ddd

import delta.EventReducer

/**
  * Type-class for entity.
  */
abstract class Entity[T, S >: Null, EVT](reducer: EventReducer[S, EVT]) {

  type Id

  type Entity = T
  type State = delta.ddd.State[S, EVT]
  type Event = EVT

  def newState(initState: S = null): State = new State(reducer, initState)

  private[ddd] def getState(e: Entity): State = {
    val s = state(e)
    validate(s.curr)
    s
  }

  private[ddd] def initEntity(initState: S, mergeEvents: List[EVT]): Entity =
    init(newState(initState), mergeEvents)

  /**
    * Initialize entity instance.
    * @param state The internal state
    * @param mergeEVTs Any potential events to merge
    * @return The Entity instance
    */
  protected def init(state: State, mergeEvents: List[Event]): Entity

  /**
    * Get state used by the entity instance.
    * @param instance The instance to get mutator from
    */
  protected def state(entity: Entity): State

  /**
    * Validate invariants. Convenience method
    * for unifying invariant validation to
    * a single place. Ideally, checks should happen
    * at every state transition, but this is not
    * always convenient.
    */
  protected def validate(state: S): Unit

}
