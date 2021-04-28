package delta.process

import scala.concurrent._
import scuff.concurrent._

/**
  * Codec with asynchrounous `encode` method.
  * Some derived state might rely on external processes,
  * or can simply be expensive to generate.
  */
abstract class AsyncCodec[W, S] extends Serializable {
  def encode(ws: W)/*(implicit ec: ExecutionContext)*/: Future[S]
  def decode(ss: S): W

  final def encode(ws: Option[W])/*(implicit ec: ExecutionContext)*/: Future[Option[S]] =
    if (ws.isEmpty) Future.none
    else encode(ws.get)/*(ec)*/.map(Some(_))(Threads.PiggyBack)
  final def decode(ss: Option[S]): Option[W] =
    if (ss.isEmpty) None
    else Some(decode(ss.get))

  @inline
  protected implicit final def successful(ss: S): Future[S] = Future successful ss
}

private[process] final class SyncCodec[W, S](sync: scuff.Codec[W, S])
extends AsyncCodec[W, S] {
  def encode(ws: W)/*(implicit ec: ExecutionContext)*/: Future[S] = successful(sync encode ws)
  def decode(ss: S): W = sync decode ss
}

object AsyncCodec {

  private[delta] final object NoOp extends AsyncCodec[Any, Any] {
    def encode(ws: Any)/*(implicit ec: ExecutionContext)*/: Future[Any] = successful(ws)
    def decode(ss: Any): Any = ss
  }

  implicit def noop[S] = NoOp.asInstanceOf[AsyncCodec[S, S]]

  def apply[W, S](codec: scuff.Codec[W, S]): AsyncCodec[W, S] = new SyncCodec(codec)

}
