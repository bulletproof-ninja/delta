package delta

import scuff._
import scala.reflect.{ ClassTag, classTag }

/**
  * Generic state projector.
  * @tparam S state type
  * @tparam EVT event type
  */
abstract class Projector[S >: Null: ClassTag, EVT: ClassTag]
extends Serializable {

  /** Handle first event. */
  def init(evt: EVT): S
  /** Handle subsequent event(s). */
  def next(state: S, evt: EVT): S

  type Transaction = delta.Transaction[_, _ >: EVT]

  def apply[A](
      tx: Transaction, state: Option[A])(
      implicit ev: S <:< A): S = try {
    tx.events.iterator
      .collectAs[EVT]
      .foldLeft(state.orNull.asInstanceOf[S])(delegate)
  } catch {
    case _: ClassCastException =>
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} was expecting `${classTag[S].runtimeClass.getName}`, but got `${state.get.getClass.getName}`")
  }

  @inline
  private def delegate(stateOrNull: S, evt: EVT): S = {
    stateOrNull match {
      case null => init(evt)
      case state => next(state, evt)
    }
  }

}

object Projector {

  def apply[ID, S >: Null, EVT](
      getProjector: Transaction[ID, _ >: EVT] => Projector[S, EVT])(
      tx: Transaction[ID, _ >: EVT], state: Option[S]): S =
    getProjector(tx).apply(tx, state)

}
