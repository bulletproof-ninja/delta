package delta.ddd

/**
  *  Generic state/event fold.
  */
trait Fold[S, -EVT] {
  def init(evt: EVT): S
  def next(state: S, evt: EVT): S
}
