package delta.redis

import _root_.redis.clients.jedis.Protocol
import java.nio.charset.Charset

object RedisEncoder {
  private[this] val charset = Charset.forName(Protocol.CHARSET)
  @inline
  def encode(str: String) = str.getBytes(charset)
  @inline
  def encode(bytes: Array[Byte], offset: Int = 0, length: Int = -1) = {
    val len = if (length == -1) bytes.length - offset else length
    new String(bytes, offset, len, charset)
  }
}
