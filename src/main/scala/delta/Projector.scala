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
  extends ((Transaction[_, _ >: EVT], Option[M]) => M)
  with Serializable {

  type TXN = Transaction[_, _ >: EVT]

  def apply(txn: TXN, st: Option[M]): M

}

object TransactionProjector {

  def apply[T >: Null: ClassTag, EVT: ClassTag](
    projector: Projector[T, EVT]): TransactionProjector[T, EVT] =
    this.apply(projector, Codec.noop[T])

  def apply[T >: Null: ClassTag, EVT: ClassTag](
    newProjector: Transaction[_, _ >: EVT] => Projector[T, EVT]): TransactionProjector[T, EVT] =
    this.apply(Codec.noop[T])(newProjector)

  def apply[M >: Null: ClassTag, S >: Null, EVT: ClassTag](
    projector: Projector[S, EVT], codec: Codec[M, S]): TransactionProjector[M, EVT] =
    this.apply(codec)(_ => projector)

  def apply[M >: Null: ClassTag, S >: Null, EVT: ClassTag](
    codec: Codec[M, S])(
    newProjector: Transaction[_, _ >: EVT] => Projector[S, EVT]): TransactionProjector[M, EVT] =

    new TransactionProjector[M, EVT] {
      def apply(tx: Transaction[_, _ >: EVT], m: Option[M]): M =
        process[M, S, EVT](newProjector(tx), codec)(tx, m)
    }

  def process[S >: Null, EVT: ClassTag](
    projector: Projector[S, EVT])(
    tx: Transaction[_, _ >: EVT], st: Option[S]): S =
      process[S, S, EVT](projector, Codec.noop[S])(tx, st)

  def process[M >: Null, S >: Null, EVT: ClassTag](
    projector: Projector[S, EVT],
    codec: Codec[M, S])(
    tx: Transaction[_, _ >: EVT], m: Option[M]): M = {

    val initState = m.orNull match {
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
