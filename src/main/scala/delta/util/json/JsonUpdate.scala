package delta.util.json

import scuff.Codec
import delta.process.Update
import scuff._
import scuff.json._, JsVal._

object JsonUpdate {

  private final val DefaultContentFieldName = "update"

  def apply[U](
      contentJsonCodec: Codec[U, String],
      contentFieldName: String = DefaultContentFieldName): Codec[Update[U], String] =
    new JsonUpdate(contentJsonCodec, contentFieldName)

  def fromBinary[U](
      contentJsonCodec: Codec[U, Array[Byte]],
      contentFieldName: String = DefaultContentFieldName): Codec[Update[U], String] =
    apply(Codec.pipe(contentJsonCodec, Codec.UTF8.reverse), contentFieldName)

}

/**
 * @tparam U Update type
 */
class JsonUpdate[U](contentJsonCodec: Codec[U, String], contentFieldName: String)
  extends Codec[Update[U], String] {

  require(contentFieldName != "tick" && contentFieldName != "revision", s"Invalid content field name: $contentFieldName")

  def this(contentJsonCodec: Codec[U, String]) =
    this(contentJsonCodec, JsonUpdate.DefaultContentFieldName)

  def encode(update: Update[U]): String = {
    val contentField: String = update.changed match {
      case Some(content) => s""","$contentFieldName":${contentJsonCodec encode content}"""
      case _ => ""
    }
    val revisionField = if (update.revision < 0) "" else s""","revision":${update.revision}"""
    s"""{"tick":${update.tick}$revisionField$contentField}"""
  }

  def decode(json: String): Update[U] = this decode (JsVal parse json).asObj

  private[json] def decode(ast: JsObj): Update[U] = {
    val tick = ast.tick.asNum
    val revision = ast.revision getOrElse JsNum(-1)
    val content = ast(contentFieldName) match {
      case JsUndefined | JsNull => None
      case ast => Some { contentJsonCodec decode ast.toJson }
    }
    new Update(content, revision.toInt, tick.toLong)
  }

}
