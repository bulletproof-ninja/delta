package delta.util.json

import scuff.Codec
import scuff.Numbers

class Tuple2Codec[A, B](a: Codec[A, JSON], b: Codec[B, JSON])
  extends Codec[(A, B), JSON] {

  def encode(t: (A, B)): JSON = {
    val jsonA = a encode t._1
    val jsonB = b encode t._2
    s"[$jsonA,$jsonB,${jsonA.length},${jsonB.length}]"
  }
  def decode(json: JSON): (A, B) = {
    import Tuple2Codec._

    val idxLenB = json.lastIndexOf(',', json.length - 3) + 1
    val lenB = Numbers.parseUnsafeInt(json, idxLenB)(EndBracket)
    val idxLenA = json.lastIndexOf(',', idxLenB - 3) + 1
    val lenA = Numbers.parseUnsafeInt(json, idxLenA)(Comma)

    val jsonA = json.substring(1, 1 + lenA)
    val jsonB = json.substring(2 + lenA, 2 + lenA + lenB)
    (a decode jsonA, b decode jsonB)
  }
}

object Tuple2Codec {

  def apply[A, B](a: Codec[A, JSON], b: Codec[B, JSON]): Codec[(A, B), JSON] = new Tuple2Codec(a, b)
  def apply[T](t: Codec[T, JSON]): Codec[(T, T), JSON] = new Tuple2Codec(t, t)

}
