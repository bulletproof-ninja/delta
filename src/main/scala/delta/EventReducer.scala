package delta

import scuff._
import scala.reflect.ClassTag

/**
  *  Generic state reducer.
  */
trait EventReducer[S, EVT] extends Serializable {
  def init(evt: EVT): S
  def next(state: S, evt: EVT): S
}

object EventReducer {
  def process[S, EVT: ClassTag](
      reducer: EventReducer[S, EVT])(
      os: Option[S], events: List[_ >: EVT]): S = {
    val filtered = events.collect {
      case evt: EVT => evt
    }
    val (state, evts) = os.map(_ -> filtered) getOrElse (reducer.init(filtered.head) -> filtered.tail)
    if (evts.isEmpty) state
    else evts.foldLeft(state)(reducer.next)
  }
}

final class EventReducerAdapter[S1, S2, EVT: ClassTag](reducer: EventReducer[S2, EVT], codec: Codec[S1, S2])
  extends EventReducer[S1, EVT] {
  def init(e: EVT) = codec decode reducer.init(e)
  def next(s: S1, e: EVT) = codec decode reducer.next(codec encode s, e)
}
