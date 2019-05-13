package delta

import scuff._
import scala.reflect.ClassTag

/**
 * Generic state projector.
 * @tparam S state type
 * @tparam EVT event type
 */
trait Projector[S, EVT] extends Serializable {
  /** Initial event. */
  def init(evt: EVT): S
  /** Subsequent event(s). */
  def next(state: S, evt: EVT): S

  def apply(evt: EVT)(state: Option[S]): S = state match {
    case Some(state) => next(state, evt)
    case _ => init(evt)
  }

}

object Projector {

  def apply[S, EVT](initF: EVT => S)(nextF: S => EVT => S): Projector[S, EVT] = new Projector[S, EVT] {
    def init(evt: EVT): S = initF(evt)
    def next(state: S, evt: EVT): S = nextF(state)(evt)
  }

  def process[S >: Null: ClassTag, EVT: ClassTag](
      projector: Projector[S, EVT])(
      os: Option[_ >: S], events: List[_ >: EVT]): S = process[S, EVT, S](projector, Codec.noop)(os, events)

  def process[S1 >: Null: ClassTag, EVT: ClassTag, S2 >: Null](
      projector: Projector[S2, EVT],
      codec: Codec[S1, S2])(
      os: Option[_ >: S1], events: List[_ >: EVT]): S1 = {

    val initState = os.collect { case state: S1 => state }
    val state = events.iterator
      .collect { case (evt: EVT) => evt }
      .foldLeft(initState.map(codec.encode).orNull) {
        case (state, evt) =>
          if (state == null) projector.init(evt)
          else projector.next(state, evt)
      }
    if (state != null) codec decode state
    else {
      val projectorName = projector.getClass.getName
      val filteredEvents = events.collect { case (evt: EVT) => evt }.mkString("\n")
      throw new IllegalStateException(s"$projectorName produced `null` state on events:\n$filteredEvents")
    }
  }

}
