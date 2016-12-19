package ulysses.ddd

/**
  * This trait represents encapsulation and transformation
  * of domain state, based on events.
  */
trait StateMutator[EVT, S <: AnyRef] {

  @inline
  private[ddd] final def mutate(evt: EVT): Unit = process(evt)
  private[this] var applied = List.empty[EVT]
  private[ddd] final def appliedEvents = applied.reverse

  /**
    * Process event and mutate internal state.
    * NOTE: This should be implemented, but not
    * called directly; instead call `apply`.
    */
  protected def process(evt: EVT): Unit

  /**
    *  Apply event.
    *  This will mutate state and collect event.
    */
  def apply(evt: EVT): Unit = {
    process(evt)
    applied = evt :: applied
  }

  /** Current state. */
  def state: S
}
