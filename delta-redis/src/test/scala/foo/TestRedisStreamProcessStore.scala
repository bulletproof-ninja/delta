package foo

import delta.testing.TestStreamProcessStore
import delta.redis._

import delta.util.json
import scuff.Codec
import redis.clients.jedis.JedisPool
import delta.process.StreamProcessStore

class TestRedisStreamProcessStore extends TestStreamProcessStore {

  val jedisProvider = new JedisProvider(new JedisPool)
  val jsonStringCodec = Codec[String, String](str => s""""$str"""", jsonStr => jsonStr.substring(1, jsonStr.length - 1))

  val snapshotVersion: Short = 1

  override def storeSupportsConditionalWrites = false
  override def newStore(): StreamProcessStore[Long, String, String] = {
    new RedisStreamProcessStore[Long, String, String](
      keyCodec = Codec.fromString(_.toLong),
      snapshotCodec = json.JsonSnapshot(jsonStringCodec),
      s"${getClass.getSimpleName}:$snapshotVersion", ec)(jedisProvider)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    jedisProvider(_.flushAll)
  }

}
