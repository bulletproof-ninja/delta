package delta.redis

import _root_.redis.clients.jedis.Protocol
import java.nio.charset.Charset

object RedisCodec extends RedisCodec

class RedisCodec extends scuff.Codec[String, Array[Byte]] {
  private[this] val _charset = Charset.forName(Protocol.CHARSET)
  final def encode(str: String): Array[Byte] = str.getBytes(_charset)
  final def decode(bytes: Array[Byte]): String = decode(bytes, 0, -1)
  final def decode(bytes: Array[Byte], offset: Int): String = decode(bytes, offset, -1)
  final def decode(bytes: Array[Byte], offset: Int, length: Int): String = {
    val len = if (length == -1) bytes.length - offset else length
    new String(bytes, offset, len, _charset)
  }
}
