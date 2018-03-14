package delta.util

import scuff.Codec
import delta.Snapshot
import scuff._

private object SnapshotJsonCodec {
  val StopAtComma = new Numbers.Stopper {
    def apply(c: Char) = c == ','
  }
}

class SnapshotJsonCodec[T](contentJsonCodec: Codec[T, String])
  extends Codec[Snapshot[T], String] {

  import SnapshotJsonCodec._

  def encode(snapshot: Snapshot[T]): String = {
    val jsonContent = contentJsonCodec encode snapshot.content
    s"""{"revision":${snapshot.revision},"tick":${snapshot.tick},"snapshot":$jsonContent}"""
  }

  def decode(json: String): Snapshot[T] = {
    val revision = json.unsafeInt(StopAtComma, 12)
    val tickPos = json.indexOf("\"tick\":", 14)
    val tick = json.unsafeLong(StopAtComma, tickPos + "\"tick\":".length)
    val snapshotPos = json.indexOf("\"snapshot\":", 16)
    val jsonContent = json.substring(snapshotPos + "\"snapshot\":".length, json.length - 1)
    val content = contentJsonCodec decode jsonContent
    new Snapshot(content, revision, tick)
  }

}
