package ulysses.ddd

/**
  * This class encapsulates and transforms domain state,
  * based on events.
  */
trait StateMutator[EVT, S <: AnyRef] {
  private[this] var applied = Vector.empty[EVT]
  def appliedEvents = applied

  final def apply(evt: EVT): Unit = {
    mutate(evt)
    applied :+= evt
  }

  /** Mutate state. */
  def mutate(evt: EVT): Unit

  /** Current state. */
  def state: S
}
