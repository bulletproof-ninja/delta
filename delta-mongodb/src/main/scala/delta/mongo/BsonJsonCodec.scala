package delta.mongo

import collection.JavaConverters._

import scuff.Codec
import org.bson.BsonValue
import org.bson.json.JsonWriterSettings
import org.bson.BsonDocument
import org.bson.BsonArray
import org.bson.BsonString
import org.bson.BsonBoolean
import org.bson.BsonNull
import org.bson.BsonDecimal128
import org.bson.types.Decimal128
import org.bson.json.JsonMode

object BsonJsonCodec
  extends BsonJsonCodec(
    JsonWriterSettings.builder()
      .newLineCharacters("")
      .indent(true)
      .indentCharacters("")
      .outputMode(JsonMode.RELAXED)
      .build())

class BsonJsonCodec(writerSettings: JsonWriterSettings)
  extends Codec[BsonValue, String] {

  import org.bson.BsonType._

  def encode(bson: BsonValue): String = bson.getBsonType match {
    case DOCUMENT => bson.asDocument.toJson(writerSettings)
    case ARRAY => bson.asArray.asScala.map(this.encode).mkString("[", ",", "]")
    case STRING => s""""${bson.asString.getValue}""""
    case BOOLEAN => if (bson.asBoolean.getValue) "true" else "false"
    case INT32 =>
      bson.asInt32.getValue.toString
    case INT64 =>
      bson.asInt32.getValue.toString
    case DECIMAL128 =>
      bson.asDecimal128.getValue.bigDecimalValue.toPlainString
    case DOUBLE =>
      bson.asDouble.getValue.toString
    case NULL => "null"
    case bsonType => sys.error(s"$bsonType is incompatible with JSON")
  }

  def decode(json: String): BsonValue = json.trim.charAt(0) match {
    case '{' => BsonDocument parse json
    case '[' => BsonArray parse json
    case '"' => new BsonString(json.substring(1, json.length - 1))
    case 't' => BsonBoolean.TRUE
    case 'f' => BsonBoolean.FALSE
    case 'n' => BsonNull.VALUE
    case _ => new BsonDecimal128(Decimal128 parse json)
  }

}
