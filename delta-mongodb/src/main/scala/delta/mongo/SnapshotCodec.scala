package delta.mongo

import org.bson.codecs.Codec
import org.bson.BsonWriter
import delta.Snapshot
import org.bson.codecs.EncoderContext
import org.bson.BsonReader
import org.bson.codecs.DecoderContext
import org.bson.codecs.configuration.CodecRegistry
import scala.reflect.{ ClassTag, classTag }
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.bson.codecs.BinaryCodec
import org.bson.types.Binary
import org.bson.BsonBinarySubType

private class SnapshotCodec[MongoContent, SnapshotContent] private (
  mongoCodec: Codec[MongoContent], scuffCodec: scuff.Codec[SnapshotContent, MongoContent])
    extends Codec[Snapshot[SnapshotContent]] {

  def getEncoderClass() = classOf[Snapshot[SnapshotContent]]

  def encode(writer: BsonWriter, snapshot: Snapshot[SnapshotContent], ctx: EncoderContext) {
    writer.writeStartDocument()
    writer.writeInt32("rev", snapshot.revision)
    writer.writeInt64("tick", snapshot.tick)
    writer.writeName("content")
    mongoCodec.encode(writer, scuffCodec encode snapshot.content, ctx)
    writer.writeEndDocument()
  }
  def decode(reader: BsonReader, ctx: DecoderContext): Snapshot[SnapshotContent] = {
    reader.readStartDocument()
    val revision = reader.readInt32("rev")
    val tick = reader.readInt64("tick")
    reader.readName("content")
    val content = scuffCodec decode mongoCodec.decode(reader, ctx)
    reader.readEndDocument()
    new Snapshot(content, revision, tick)
  }
}

object SnapshotCodec {

  private final class BinaryAdapterCodec[T](
    scuffCodec: scuff.Codec[T, Array[Byte]],
    subType: BsonBinarySubType)
      extends scuff.Codec[T, Binary] {
    def encode(obj: T): Binary = new Binary(subType, scuffCodec encode obj)
    def decode(bin: Binary): T = scuffCodec decode bin.getData
  }

  def apply[T](codec: Codec[T]): Codec[Snapshot[T]] = {
    new SnapshotCodec(codec, scuff.Codec.noop)
  }

  def fromRegistry[T: ClassTag](reg: CodecRegistry): Codec[Snapshot[T]] = {
    val mongoCodec = reg.get(classTag[T].runtimeClass.asInstanceOf[Class[T]])
    new SnapshotCodec(mongoCodec, scuff.Codec.noop)
  }

  def asDocument[T](scuffCodec: scuff.Codec[T, Document], reg: CodecRegistry = null): Codec[Snapshot[T]] = {
    val docCodec = reg match {
      case null => new DocumentCodec
      case reg => reg.get(classOf[Document])
    }
    new SnapshotCodec(docCodec, scuffCodec)
  }

  def asBinary[T](
    scuffCodec: scuff.Codec[T, Array[Byte]] = new scuff.JavaSerializer[T],
    reg: CodecRegistry = null,
    subType: BsonBinarySubType = BsonBinarySubType.BINARY): Codec[Snapshot[T]] = {
    val binCodec = reg match {
      case null => new BinaryCodec
      case reg => reg.get(classOf[Binary])
    }
    new SnapshotCodec(binCodec, new BinaryAdapterCodec(scuffCodec, subType))
  }
}
