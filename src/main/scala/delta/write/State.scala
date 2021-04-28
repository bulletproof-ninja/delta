package delta.write

import delta.Projector

/**
  * State reference. Encapsulates and transforms
  * immutable state by applying events.
  */
final class StateRef[S >: Null, EVT] private[write] (
    projector: Projector[S, EVT],
    private[this] var _state: S) {

  @inline // Just mutate, don't collect event
  private[write] final def mutate(evt: EVT): Unit = fold(evt)
  private[this] var _applied: List[EVT] = Nil
  final def appliedEvents =
    if (_applied.isEmpty || _applied.tail.isEmpty) _applied
    else _applied.reverse

  @inline
  private def fold(evt: EVT): Unit = {
    _state = _state match {
      case null => projector.init(evt)
      case state => projector.next(state, evt)
    }
  }

  /**
    *  Apply event.
    *  This will mutate state and collect event.
    */
  def apply(evt: EVT): Unit = {
    fold(evt)
    _applied = evt :: _applied
  }

  /** Current state. */
  def get: S = _state

  override def toString =
    if (_state == null) "<uninitialized>"
    else _applied.length match {
      case 1 =>   s"After 1 event: ${_state}"
      case len => s"After $len events: ${_state}"
    }

}
