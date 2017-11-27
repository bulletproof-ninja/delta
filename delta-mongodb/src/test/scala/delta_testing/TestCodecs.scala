package delta_testing

import java.nio.ByteBuffer

import org.bson.{ BsonBinaryReader, BsonBinaryWriter }
import org.bson.io.BasicOutputBuffer
import org.junit._, Assert._

import delta.mongo.ScalaEnumCodec

class TestCodecs {

  @Test
  def scala_enum() {
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

}
