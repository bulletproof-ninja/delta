package delta.ddd

/**
  * Type-class for entity.
  */
trait Entity {

  type Type
  type Id
//  type Event
//  type State >: Null
  type Mutator <: StateMutator

  private[ddd] def getMutator(e: Type): Mutator = {
    val mutator = done(e)
    checkInvariants(mutator.state)
    mutator
  }
  
  /**
    * Instantiate new mutator, with existing state or from scratch.
    * @param state Optional state
    */
  def newMutator: Mutator
  
  /**
    * Initialize entity instance.
    * @param state The internal state
    * @param mergeEvents Any potential events to merge
    * @return The Entity instance
    */
  def init(mutator: Mutator, mergeEvents: List[Mutator#Event]): Type

  /**
    * Get the mutator used for the entity instance.
    * @param instance The instance to get mutator from
    */
  protected def done(instance: Type): Mutator

  /**
    * Convenience method for ensuring invariants
    * are not violated. Defaults to no-op.
    */
  protected def checkInvariants(state: Mutator#State): Unit = ()
}
