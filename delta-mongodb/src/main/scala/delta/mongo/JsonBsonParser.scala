package delta.mongo

import org.bson._
import org.bson.types.Decimal128

class JsonBsonParser(json: CharSequence, offset: Int = 0)
  extends scuff.json.AbstractParser(json, offset) {

  type JsVal = BsonValue
  type JsBool = BsonBoolean
  def True: JsBool = BsonBoolean.TRUE
  def False: JsBool = BsonBoolean.FALSE
  type JsObj = BsonDocument
  def JsObj(m: Map[String, JsVal]): JsObj = m.foldLeft(new BsonDocument) {
    case (doc, (key, value)) => doc.put(key, value); doc
  }
  type JsArr = BsonArray
  def JsArr(values: Seq[JsVal]): JsArr = values.foldLeft(new BsonArray) {
    case (arr, value) => arr.add(value); arr
  }
  type JsStr = BsonString
  def JsStr(s: String): JsStr = new BsonString(s)
  type JsNull = BsonNull
  def JsNull: JsNull = BsonNull.VALUE
  type JsNum = BsonNumber
  def JsNum(n: Number): JsNum = n match {
    case l: java.lang.Long =>
      if (l <= Int.MaxValue && l >= Int.MinValue) new BsonInt32(l.intValue)
      else new BsonInt64(l)
    case d: java.lang.Double => new BsonDouble(d)
    case bd: BigDecimal => new BsonDecimal128(new Decimal128(bd.underlying))
    case other => new BsonDecimal128(Decimal128.parse(other.toString)) // Should not happen
  }
  val Zero: JsNum = new BsonInt32(0)

}
