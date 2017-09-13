package delta

import scuff.Codec
import scala.reflect.ClassTag

/**
  *  Generic fold.
  */
abstract class Fold[S, E: ClassTag] extends Serializable {

  def init(e: E): S
  def next(s: S, e: E): S

  def process(os: Option[S], te: Traversable[_ >: E]): S = {
    val verified = verifyEvents(te)
    val (s, evts) = os.map(_ -> verified) getOrElse (init(verified.head) -> verified.tail)
    if (evts.isEmpty) s
    else evts.foldLeft(s) {
      case (s, e) => next(s, e)
    }
  }
  protected def verifyEvents(evts: Traversable[_ >: E]): Traversable[E] = evts.collect { case e: E => e }
}

final class FoldAdapter[S1, S2, E: ClassTag](fold: Fold[S2, E], codec: Codec[S1, S2])
    extends Fold[S1, E] {
  def init(e: E) = codec decode fold.init(e)
  def next(s: S1, e: E) = codec decode fold.next(codec encode s, e)
}
