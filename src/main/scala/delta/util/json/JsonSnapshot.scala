package delta.util.json

import scuff.Codec
import delta.Snapshot
import scuff._
import scuff.json._, JsVal._

object JsonSnapshot {

  private final val DefaultContentFieldName = "snapshot"

  def apply[T](
      contentJsonCodec: Codec[T, String],
      contentFieldName: String = DefaultContentFieldName): Codec[Snapshot[T], String] =
    new JsonSnapshot(contentJsonCodec, contentFieldName)

  def fromBinary[T](
      contentJsonCodec: Codec[T, Array[Byte]],
      contentFieldName: String = DefaultContentFieldName): Codec[Snapshot[T], String] =
    apply(Codec.pipe(contentJsonCodec, Codec.UTF8.reverse), contentFieldName)

}

/**
 * @tparam T Snapshot type
 */
class JsonSnapshot[T](contentJsonCodec: Codec[T, String], contentFieldName: String)
  extends Codec[Snapshot[T], String] {

  require(contentFieldName != "tick" && contentFieldName != "revision", s"Invalid content field name: $contentFieldName")

  def this(contentJsonCodec: Codec[T, String]) =
      this(contentJsonCodec, JsonSnapshot.DefaultContentFieldName)

  def encode(snapshot: Snapshot[T]): String = {
    val contentField: String = s""","$contentFieldName":${contentJsonCodec encode snapshot.state}"""
    val revisionField = if (snapshot.revision < 0) "" else s""","revision":${snapshot.revision}"""
    s"""{"tick":${snapshot.tick}$revisionField$contentField}"""
  }

  def decode(json: String): Snapshot[T] = this decode (JsVal parse json).asObj

  private[json] def decode(ast: JsObj): Snapshot[T] = {
    val tick = ast.tick.asNum
    val revision = ast.revision getOrElse JsNum(-1)
    val content = contentJsonCodec decode ast(contentFieldName).toJson
    new Snapshot(content, revision.toInt, tick.toLong)
  }

}
