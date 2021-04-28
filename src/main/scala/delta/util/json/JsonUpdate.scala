package delta.util.json

import scuff.Codec
import delta.process.Update
import scuff.json._, JsVal._

object JsonUpdate {

  private final val DefaultContentFieldName = "update"

  def apply[U](
      contentJsonCodec: Codec[U, JSON],
      contentFieldName: String = DefaultContentFieldName): Codec[Update[U], JSON] =
    new JsonUpdate(contentJsonCodec, contentFieldName)

  def fromBinary[U](
      contentJsonCodec: Codec[U, Array[Byte]],
      contentFieldName: String = DefaultContentFieldName): Codec[Update[U], JSON] =
    apply(Codec.pipe(contentJsonCodec, Codec.UTF8.reverse), contentFieldName)

}

/**
 * @tparam U Update type
 */
class JsonUpdate[U](contentJsonCodec: Codec[U, JSON], contentFieldName: String)
  extends Codec[Update[U], JSON] {

  require(contentFieldName != "tick" && contentFieldName != "revision", s"Invalid content field name: $contentFieldName")

  def this(contentJsonCodec: Codec[U, JSON]) =
    this(contentJsonCodec, JsonUpdate.DefaultContentFieldName)

  def encode(update: Update[U]): JSON = {
    val contentField: String = update.change match {
      case Some(content) => s""","$contentFieldName":${contentJsonCodec encode content}"""
      case _ => ""
    }
    val revisionField = if (update.revision < 0) "" else s""","revision":${update.revision}"""
    s"""{"tick":${update.tick}$revisionField$contentField}"""
  }

  def decode(json: JSON): Update[U] = this decode (JsVal parse json).asObj

  private[json] def decode(ast: JsObj): Update[U] = {
    val tick = ast.tick.asNum
    val revision = ast.revision || JsNum(-1)
    val content = ast(contentFieldName) match {
      case JsUndefined | JsNull => None
      case ast => Some { contentJsonCodec decode ast.toJson }
    }
    new Update(content, revision.toInt, tick.toLong)
  }

}
