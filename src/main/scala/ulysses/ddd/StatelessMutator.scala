package ulysses.ddd

/** Stateless mutator. */
private[ddd] final class StatelessMutator[EVT, S <: AnyRef](newMutator: Option[S] => StateMutator[EVT, S]) {
  def init(evt: EVT): S = {
    val m = newMutator(None)
    m.mutate(evt)
    m.state
  }
  def next(s: S, evt: EVT) = {
    val m = newMutator(Some(s))
    m.mutate(evt)
    m.state
  }
}
