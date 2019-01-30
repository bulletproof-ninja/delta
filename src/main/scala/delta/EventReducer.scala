package delta

import scuff._
import scala.reflect.ClassTag

/**
  *  Generic event reducer.
  */
trait EventReducer[S, EVT] extends Serializable {
  def init(evt: EVT): S
  def next(state: S, evt: EVT): S
}

object EventReducer {
  def process[S >: Null, EVT: ClassTag](
      reducer: EventReducer[S, EVT])(
      os: Option[S], events: List[_ >: EVT]): S = process[S, EVT, S](reducer, Codec.noop)(os, events)

  def process[S1 >: Null, EVT: ClassTag, S2 >: Null](
      reducer: EventReducer[S2, EVT],
      codec: Codec[S1, S2])(
      os: Option[S1], events: List[_ >: EVT]): S1 = {

    codec decode {
      events.iterator.foldLeft(os.map(codec.encode).orNull) {
        case (null, evt: EVT) => reducer.init(evt)
        case (state, evt: EVT) => reducer.next(state, evt)
      }
    }
  }

}
