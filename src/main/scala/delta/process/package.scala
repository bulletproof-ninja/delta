package delta

import scala.concurrent.Future
import scuff.concurrent.Threads

/**
  * Tools for consuming and processing events.
  * @note This package assumes immutable state and
  * ''will not'' work correctly if mutable state is
  * used.
  */
package object process {

  type UpdateHub[ID, U] = MessageHub[ID, Update[U]]
  type UpdateSource[ID, U] = MessageSource[ID, Update[U]]

  implicit def toAsyncCodec[A, B](codec: scuff.Codec[A, B]): AsyncCodec[A, B] = AsyncCodec(codec)

  private[process] val RightFalse = Right(false)

  // private[delta] def recoverError[K](
  //     key: K,
  //     future: Future[Unit],
  //     errors: Map[K, Throwable] = Map.empty[K, Throwable])
  //     : Future[Map[K, Throwable]] = {
  //   implicit def ec = Threads.PiggyBack
  //   future
  //     .map { _ => errors }
  //     .recover { case e: Exception => errors.updated(key, e) }
  // }


  private[this] val unit = new UpdateCodec[Any, Unit] {
    def asUpdate(prevState: Option[Any], currState: Any): Unit = ()
    def updateState(prevState: Option[Any], update: Unit): Option[Any] = scala.None
  }
  private[this] val noop = new UpdateCodec[Any, Any] {
    def asUpdate(prevState: Option[Any], currState: Any): Any = currState
    def updateState(prevState: Option[Any], update: Any): Option[Any] = Some(update)
  }

  implicit def updateIsUnit[S] = unit.asInstanceOf[UpdateCodec[S, Unit]]
  implicit def updateIsState[S] = noop.asInstanceOf[UpdateCodec[S, S]]

}
