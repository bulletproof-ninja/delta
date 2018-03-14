package delta.ddd

import delta.EventReducer

/**
  * This class encapsulates and transforms
  * domain state, based on events.
  */
final class State[S >: Null, EVT] private[ddd] (reducer: EventReducer[S, EVT], private[this] var _state: S) {

  @inline
  private[ddd] final def mutate(evt: EVT): Unit = reduce(evt)
  private[this] var _applied = List.empty[EVT]
  private[ddd] final def appliedEvents = _applied match {
    case one @ _ :: Nil => one
    case more => more.reverse
  }

  @inline
  private def reduce(evt: EVT): Unit = {
    _state = _state match {
      case null => reducer.init(evt)
      case state => reducer.next(state, evt)
    }
  }

  /**
    *  Apply event.
    *  This will mutate state and collect event.
    */
  def apply(evt: EVT): Unit = {
    reduce(evt)
    _applied = evt :: _applied
  }

  /** Current state. */
  def curr: S = _state
}
