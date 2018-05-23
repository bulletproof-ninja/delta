package delta.util

import scuff.Codec
import delta.Snapshot
import scuff._

object SnapshotJsonCodec {
  private val StopAtComma = new Numbers.Stopper {
    def apply(c: Char) = c == ','
  }

  def apply[T](contentJsonCodec: Codec[T, String]) = new SnapshotJsonCodec(contentJsonCodec)
  def fromBinary[T](contentJsonCodec: Codec[T, Array[Byte]]) = apply(Codec.pipe(contentJsonCodec, Codec.UTF8.reverse))

}

class SnapshotJsonCodec[T](contentJsonCodec: Codec[T, String], contentFieldName: String)
  extends Codec[Snapshot[T], String] {

  require(contentFieldName != "tick" && contentFieldName != "rev", "Invalid content field name")

  def this(contentJsonCodec: Codec[T, String]) = this(contentJsonCodec, "data")

  private[this] val contentFieldPrefix = s""""$contentFieldName":"""

  import SnapshotJsonCodec.StopAtComma

  def encode(snapshot: Snapshot[T]): String = {
    val jsonContent = contentJsonCodec encode snapshot.content
    if (snapshot.revision < 0) {
      s"""{"tick":${snapshot.tick},"$contentFieldName":$jsonContent}"""
    } else {
      s"""{"rev":${snapshot.revision},"tick":${snapshot.tick},"$contentFieldName":$jsonContent}"""
    }
  }

  def decode(json: String): Snapshot[T] = {
    val tickIdx = tickIndex(json)
    new Snapshot(content(json, tickIdx), revision(json), tick(json, tickIdx))
  }

  /** Extract revision. */
  def revision(json: String): Int =
    if (json.charAt(2) == 'r') json.unsafeInt(StopAtComma, offset = "{\"rev\":".length) else -1
  /** Extract tick. */
  def tick(json: String): Long = tick(json, tickIndex(json))
  /** Extract parsed content data. */
  def content(json: String): T = content(json, tickIndex(json))
  /** Extract raw JSON content. */
  def rawContent(json: String): String = rawContent(json, tickIndex(json))

  private def tickIndex(json: String): Int =
    if (json.charAt(2) == 't') 1 else json.indexOf("\"tick\":", "{\"rev\":".length + 2)

  private def tick(json: String, tickIdx: Int): Long =
    json.unsafeLong(StopAtComma, offset = tickIdx + "\"tick\":".length)

  private def rawContent(json: String, tickIdx: Int): String = {
    val contentPos = json.indexOf(contentFieldPrefix, tickIdx + "\"tick\":".length + 2)
    json.substring(contentPos + contentFieldPrefix.length, json.length - 1)
  }
  private def content(json: String, tickIdx: Int): T = contentJsonCodec decode rawContent(json, tickIdx)

}
