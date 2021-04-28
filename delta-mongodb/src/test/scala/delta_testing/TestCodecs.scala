package delta.testing

import java.nio.ByteBuffer

import org.bson.{ BsonBinaryReader, BsonBinaryWriter }
import org.bson.io.BasicOutputBuffer

import delta.mongo.ScalaEnumCodec
import delta.mongo.BsonJsonCodec
import org.bson.types.Decimal128
import org.bson.BsonNull

import scala.jdk.CollectionConverters._
import org.bson.BsonDocument

class TestCodecs
extends delta.testing.BaseTest {

  test("scala_enum") {
    object Cola extends Enumeration {
      val Coke, Pepsi, RC = Value
    }
    val codec = ScalaEnumCodec(Cola)
    val out = new BasicOutputBuffer
    val writer = new BsonBinaryWriter(out)
    writer.writeStartDocument()
    writer.writeName("cola")
    codec.encode(writer, Cola.Pepsi, null)
    writer.writeEndDocument()

    val bytes = out.getInternalBuffer.take(out.getPosition)
    val reader = new BsonBinaryReader(ByteBuffer wrap bytes)
    reader.readStartDocument()
    reader.readName()
    val cola = codec.decode(reader, null)
    reader.readEndDocument()
    assert(Cola.Pepsi === cola)
  }

  test("jsonDoc") {
    val json = s"""{
  "int": 42,
  "long": ${Int.MaxValue * 2L},
  "decimal": 5463425234543263365363.34545345345,
  "zero": 0,
  "clown": "🤡",
  "escapedClown": "\\ud83e\\udd21",
  "null": null,
  "true": true,
  "false": false,
  "nan": "NaN",
  "list": [1,2,3],
  "empty": {}
}"""
    println(json)
    val bson = BsonJsonCodec encode json
    val doc = bson.asDocument()
    assert(42 === doc.getInt32("int").intValue)
    assert(Int.MaxValue * 2L === doc.getInt64("long").longValue)
    assert(Decimal128.parse("5463425234543263365363.34545345345") === doc.getDecimal128("decimal").getValue)
    assert(0 === doc.getInt32("zero").intValue)
    assert("🤡" === doc.getString("clown").getValue)
    assert("🤡" === doc.getString("escapedClown").getValue)
    assert(BsonNull.VALUE === doc.get("null"))
    assert(doc.getBoolean("true").getValue)
    assert(!doc.getBoolean("false").getValue)
    assert(java.lang.Double.parseDouble(doc.getString("nan").getValue).isNaN)
    assert(List(1,2,3) === doc.getArray("list").getValues.iterator.asScala.map(_.asInt32.intValue).toList)
    assert(new BsonDocument === doc.get("empty"))

    val jsonFromBson = BsonJsonCodec decode bson
    println(jsonFromBson)
  }

}
