package delta.util.json

import scuff.Codec
import scuff.json._, JsVal._

class Tuple2Codec[A, B](a: Codec[A, JSON], b: Codec[B, JSON])
  extends Codec[(A, B), JSON] {

  def encode(t: (A, B)): JSON = {
    val jsonA = a encode t._1
    val jsonB = b encode t._2
    s"[$jsonA,$jsonB]"
  }
  def decode(json: JSON): (A, B) = {
    val JsArr(jsA, jsB) = JsVal parse json
    val a = this.a decode jsA.toJson
    val b = this.b decode jsB.toJson
    a -> b
  }
}

object Tuple2Codec {

  def apply[A, B](a: Codec[A, JSON], b: Codec[B, JSON]): Codec[(A, B), JSON] = new Tuple2Codec(a, b)
  def apply[T](t: Codec[T, JSON]): Codec[(T, T), JSON] = new Tuple2Codec(t, t)

}
