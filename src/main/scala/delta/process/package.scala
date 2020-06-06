package delta

/**
  * Tools for consuming and processing events.
  * @note This package assumes immutable state and
  * _will not_ work correctly if mutable objects are
  * used.
  */
package object process {

  type UpdateHub[ID, U] = MessageHub[ID, Update[U]]

  implicit def asyncCodec[A, B](codec: scuff.Codec[A, B]): AsyncCodec[A, B] = AsyncCodec(codec)

  private[process] val RightFalse = Right(false)

  private[this] val unit = new UpdateCodec[Any, Unit] {
    def asUpdate(prevState: Option[Any], currState: Any): Unit = ()
    def asSnapshot(prevState: Option[Any], update: Unit): Option[Any] = scala.None
  }
  private[this] val noop = new UpdateCodec[Any, Any] {
    def asUpdate(prevState: Option[Any], currState: Any): Any = currState
    def asSnapshot(prevState: Option[Any], update: Any): Option[Any] = Some(update)
  }

  implicit def Unit[S] = unit.asInstanceOf[UpdateCodec[S, Unit]]
  implicit def Default[S] = noop.asInstanceOf[UpdateCodec[S, S]]

}
