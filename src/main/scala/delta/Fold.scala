package delta

import scuff.Codec
import scala.reflect.ClassTag

/**
  *  Generic fold.
  */
abstract class Fold[S, E: ClassTag] extends Serializable {

  def init(evt: E): S
  def next(state: S, evt: E): S

  def process(os: Option[S], te: Traversable[_ >: E]): S = {
    val verified = verifyEvents(te)
    val (state, evts) = os.map(_ -> verified) getOrElse (init(verified.head) -> verified.tail)
    if (evts.isEmpty) state
    else evts.foldLeft(state)(next)
  }
  protected def verifyEvents(evts: Traversable[_ >: E]): Traversable[E] = evts.collect { case e: E => e }
}

final class FoldAdapter[S1, S2, E: ClassTag](fold: Fold[S2, E], codec: Codec[S1, S2])
  extends Fold[S1, E] {
  def init(e: E) = codec decode fold.init(e)
  def next(s: S1, e: E) = codec decode fold.next(codec encode s, e)

  override def process(os: Option[S1], te: Traversable[_ >: E]): S1 =
    codec decode fold.process(os.map(codec.encode), te)
}
