package delta_testing

import java.nio.ByteBuffer

import org.bson.{ BsonBinaryReader, BsonBinaryWriter }
import org.bson.io.BasicOutputBuffer
import org.junit._, Assert._

import delta.mongo.ScalaEnumCodec
import delta.mongo.BsonJsonCodec
import org.bson.types.Decimal128
import org.bson.BsonNull

import collection.JavaConverters._
import org.bson.BsonDocument

class TestCodecs {

  @Test
  def scala_enum(): Unit = {
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
    assertEquals(Cola.Pepsi, cola)
  }

  @Test
  def jsonDoc(): Unit = {
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
    val bson = BsonJsonCodec decode json
    val doc = bson.asDocument()
    assertEquals(42, doc.getInt32("int").intValue)
    assertEquals(Int.MaxValue * 2L, doc.getInt64("long").longValue)
    assertEquals(Decimal128.parse("5463425234543263365363.34545345345"), doc.getDecimal128("decimal").getValue)
    assertEquals(0, doc.getInt32("zero").intValue)
    assertEquals("🤡", doc.getString("clown").getValue)
    assertEquals("🤡", doc.getString("escapedClown").getValue)
    assertEquals(BsonNull.VALUE, doc.get("null"))
    assertEquals(true, doc.getBoolean("true").getValue)
    assertEquals(false, doc.getBoolean("false").getValue)
    assertTrue(java.lang.Double.parseDouble(doc.getString("nan").getValue).isNaN)
    assertEquals(List(1,2,3), doc.getArray("list").getValues.iterator.asScala.map(_.asInt32.intValue).toList)
    assertEquals(new BsonDocument, doc.get("empty"))

    val jsonFromBson = BsonJsonCodec encode bson
    println(jsonFromBson)
  }

}
