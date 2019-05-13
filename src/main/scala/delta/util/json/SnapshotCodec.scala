package delta.util.json

import scuff.Codec
import delta.Snapshot
import scuff._
import scuff.json._, JsVal._

object SnapshotCodec {

  private final val DefaultContentFieldName = "content"

  def apply[T](
      contentJsonCodec: Codec[T, String],
      contentFieldName: String = DefaultContentFieldName): Codec[Snapshot[T], String] =
    new SnapshotCodec(contentJsonCodec, contentFieldName)

  def fromBinary[T](
      contentJsonCodec: Codec[T, Array[Byte]],
      contentFieldName: String = DefaultContentFieldName): Codec[Snapshot[T], String] =
    apply(Codec.pipe(contentJsonCodec, Codec.UTF8.reverse), contentFieldName)

}

/**
 * @tparam T Snapshot type
 */
class SnapshotCodec[T](contentJsonCodec: Codec[T, String], contentFieldName: String)
  extends Codec[Snapshot[T], String] {

  require(contentFieldName != "tick" && contentFieldName != "rev", s"Invalid content field name: $contentFieldName")

  def this(contentJsonCodec: Codec[T, String]) =
      this(contentJsonCodec, SnapshotCodec.DefaultContentFieldName)

  def encode(snapshot: Snapshot[T]): String = {
    val jsonContent: String = contentJsonCodec encode snapshot.content
    if (snapshot.revision < 0) {
      s"""{"tick":${snapshot.tick},"$contentFieldName":$jsonContent}"""
    } else {
      s"""{"rev":${snapshot.revision},"tick":${snapshot.tick},"$contentFieldName":$jsonContent}"""
    }
  }

  def decode(json: String): Snapshot[T] = this decode (JsVal parse json).asObj

  private[json] def decode(ast: JsObj): Snapshot[T] = {
    val tick = ast.tick.asNum
    val revision = ast.rev getOrElse JsNum(-1)
    val content = contentJsonCodec decode ast(contentFieldName).toJson
    new Snapshot(content, revision.toInt, tick.toLong)
  }

}
