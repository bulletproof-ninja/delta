package delta

/**
  * Tools for consuming and processing events.
  * NOTE: This package assumes immutable state and
  * _will not_ work correctly if mutable objects are
  * used.
  */
package object process {

  type UpdateHub[ID, U] = MessageHub[ID, Update[U]]

  implicit def asyncCodec[A, B](codec: scuff.Codec[A, B]): AsyncCodec[A, B] = AsyncCodec(codec)

}
