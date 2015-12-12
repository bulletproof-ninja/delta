package ulysses.ddd

/**
 * This class encapsulates and transforms domain state,
 * based on events.
 */
trait StateMutator[EVT, S] extends (EVT => Unit) {
  /** Current state. */
  def state: S
}
