package ulysses.ddd

/**
  * This class encapsulates and transforms domain state,
  * based on events.
  */
trait StateMutator[EVT, S <: AnyRef] extends (EVT => Unit) {

  private[this] var applied = Vector.empty[EVT]
  private[ddd] def appliedEvents = applied
  private[ddd] final def mutate(evt: EVT): Unit = super.apply(evt)

  /** Mutate state. */
  abstract override def apply(evt: EVT): Unit = {
    super.apply(evt)
    applied :+= evt
  }

  /** Current state. */
  def state: S
}
