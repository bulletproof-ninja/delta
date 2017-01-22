package delta

import scuff.Codec

/**
  *  Generic state/event fold.
  */
trait Fold[S, -EVT] extends Serializable {
  def process(state: Option[S], events: Traversable[EVT]): S = {
    val (st, evts) = state.map(_ -> events) getOrElse (init(events.head) -> events.tail)
    if (evts.isEmpty) st
    else evts.foldLeft(st) {
      case (state, evt) => next(state, evt)
    }
  }
  def init(evt: EVT): S
  def next(state: S, evt: EVT): S
}

final class FoldAdapter[S1, S2, -EVT](fold: Fold[S2, EVT], codec: Codec[S1, S2])
    extends Fold[S1, EVT] {
  def init(evt: EVT) = codec decode fold.init(evt)
  def next(state: S1, evt: EVT) = codec decode fold.next(codec encode state, evt)
}
