package delta.util

import scuff.Codec
import java.util.UUID
import scuff.Numbers

package object json {
  type JSON = String

  val UUIDCodec = Codec.apply[UUID, JSON](uuid => s""""$uuid"""", json => UUID fromString json.substring(1, json.length - 1))

  private[json] val EndBracket = new Numbers.Stopper { def apply(c: Char) = c == ']' }
  private[json] val Comma = new Numbers.Stopper { def apply(c: Char) = c == ',' }

  private[json] def numLength(n: Long): Int = {
    if (n > 0) math.log10(n).asInstanceOf[Int] + 1
    else if (n == 0) 1
    else if (n == Long.MinValue) 20
    else numLength(-n) + 1
  }


}
