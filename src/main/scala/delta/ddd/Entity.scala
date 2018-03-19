package delta.ddd

import delta.EventReducer

private[delta] object Entity {
  def defaultName(e: Class[_]): String = {
    e.getClass.getSimpleName.replace("$", "")
  }
}

/**
  * Type-class for entity.
  */
abstract class Entity[S >: Null, EVT](val name: String, reducer: EventReducer[S, EVT]) {

  type Id
  type Type

  type State = delta.ddd.State[S, EVT]
  type Event = EVT

  def newState(initState: S = null): State = new State(reducer, initState)

  private[ddd] def validatedState(entity: Type): State = {
    val s = state(entity)
    validate(s.curr)
    s
  }

  private[ddd] def initEntity(state: S, mergeEvents: List[EVT]): Type = init(newState(state), mergeEvents)

  /**
    * Initialize entity instance.
    * @param state The internal state
    * @param mergeEVTs Any potential events to merge
    * @return The Entity instance
    */
  protected def init(state: State, mergeEvents: List[Event]): Type

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
