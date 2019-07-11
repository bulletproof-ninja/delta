package delta

import scuff._
import scala.reflect.ClassTag

/**
 * Generic state projector.
 * @tparam S state type
 * @tparam EVT event type
 */
trait Projector[S >: Null, EVT] extends Serializable {
  /** Initial event. */
  def init(evt: EVT): S
  /** Subsequent event(s). */
  def next(state: S, evt: EVT): S

}

trait TransactionProjector[M >: Null, EVT]
  extends ((Transaction[_, _ >: EVT], Option[_ >: M]) => M)
  with Serializable {

  type TXN = Transaction[_, _ >: EVT]

  def apply(txn: TXN, st: Option[_ >: M]): M

}

object TransactionProjector {

  def apply[T >: Null: ClassTag, EVT: ClassTag](
      projector: Projector[T, EVT]): TransactionProjector[T, EVT] =
    this.apply(projector, Codec.noop)

  def apply[M >: Null: ClassTag, S >: Null, EVT: ClassTag](
      projector: Projector[S, EVT], codec: Codec[M, S]): TransactionProjector[M, EVT] =

    new TransactionProjector[M, EVT] {
      def apply(tx: Transaction[_, _ >: EVT], m: Option[_ >: M]): M = {
        val initState = m.collectAs[M].orNull match {
          case null => null
          case m => codec encode m
        }
        val state = tx.events.iterator
          .collectAs[EVT]
          .foldLeft(initState) {
            case (state, evt) =>
              if (state == null) projector.init(evt)
              else projector.next(state, evt)
          }
        if (state != null) codec decode state
        else {
          val projectorName = projector.getClass.getName
          val events = tx.events.collectAs[EVT].mkString("\n")
          throw new IllegalStateException(s"$projectorName produced `null` state on events:\n$events")
        }

      }
    }
}
