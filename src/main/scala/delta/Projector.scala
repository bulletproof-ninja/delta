package delta

import scuff._
import scala.reflect.ClassTag

/**
 * Generic state projector.
 * @tparam S state type
 * @tparam EVT event type
 */
abstract class Projector[S >: Null, EVT: ClassTag] extends Serializable {
  /** Initial event. */
  def init(evt: EVT): S
  /** Subsequent event(s). */
  def next(state: S, evt: EVT): S

  type Transaction = delta.Transaction[_, _ >: EVT]

  def apply(tx: Transaction, state: Option[S]): S =
    tx.events.iterator
      .collectAs[EVT]
      .foldLeft(state.orNull) {
        case (null, evt) => init(evt)
        case (state, evt) => next(state, evt)
      }

}

object Projector {

  def apply[ID, S >: Null, EVT: ClassTag](
      getProjector: Transaction[ID, _ >: EVT] => Projector[S, EVT])(
      tx: Transaction[ID, _ >: EVT], state: Option[S]): S =
    getProjector(tx).apply(tx, state)

}
