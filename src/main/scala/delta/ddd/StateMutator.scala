package delta.ddd

/**
  * This trait represents encapsulation and transformation
  * of domain state, based on events.
  */
trait StateMutator {

  type Event
  type State >: Null

  private[this] var _state: State = null
  private[ddd] def init(state: Option[State]): this.type = init(state.orNull)
  private[ddd] def init(state: State): this.type = {
    _state = state
    _applied = Nil
    this
  }

  @inline
  private[ddd] final def mutate(evt: Event): Unit = fold(evt)
  private[this] var _applied = List.empty[Event]
  private[ddd] final def appliedEvents = _applied match {
    case head :: Nil => _applied
    case _ => _applied.reverse
  }

  @inline
  private def fold(evt: Event): Unit = {
    _state = _state match {
      case null => fold.init(evt)
      case state => fold.next(state, evt)
    }
  }

  /** Fold instance. */
  protected def fold: Fold[State, Event]

  /**
    *  Apply event.
    *  This will mutate state and collect event.
    */
  def apply(evt: Event): Unit = {
    fold(evt)
    _applied = evt :: _applied
  }

  /** Current state. */
  def state: State = _state
}
