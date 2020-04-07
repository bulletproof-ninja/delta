package delta.mongo

import scala.jdk.CollectionConverters._

import org.bson.BsonValue

import scuff.Codec

object BsonJsonCodec extends BsonJsonCodec(false, false)

class BsonJsonCodec(escapeSlash: Boolean, upperCaseHex: Boolean)
  extends Codec[BsonValue, String] {

  import org.bson.BsonType._

  private def toJson(str: String): String =
    scuff.json.JsStr.toJson(
        str,
        escapeSlash = escapeSlash,
        upperCaseHex = upperCaseHex)

  def encode(bson: BsonValue): String = bson.getBsonType match {
    case DOCUMENT =>
      val entries = bson.asDocument.entrySet().iterator.asScala.map { entry =>
        s"""${toJson(entry.getKey)}:${encode(entry.getValue)}"""
      }
      entries.mkString("{", ",", "}")
    case ARRAY =>
      bson.asArray.iterator.asScala.map(this.encode).mkString("[", ",", "]")
    case STRING =>
      toJson(bson.asString.getValue)
    case BOOLEAN =>
      if (bson.asBoolean.getValue) "true" else "false"
    case INT32 =>
      bson.asInt32.getValue.toString
    case INT64 =>
      bson.asInt64.getValue.toString
    case DECIMAL128 =>
      bson.asDecimal128.getValue.bigDecimalValue.toPlainString
    case DOUBLE =>
      val dbl = bson.asDouble.getValue
      if (java.lang.Double.isInfinite(dbl) || java.lang.Double.isNaN(dbl)) s""""$dbl""""
      else dbl.toString
    case NULL => "null"
    case bsonType => sys.error(s"$bsonType is incompatible with JSON")
  }

  def decode(json: String): BsonValue = new JsonBsonParser(json).parse()

}
