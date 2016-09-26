package ulysses.mongo

import scala.reflect.{ ClassTag, classTag }
import org.bson.BsonReader
import org.bson.BsonWriter
import org.bson.codecs.EncoderContext
import org.bson.codecs.DecoderContext

abstract class TypeCodec[T: ClassTag]
    extends org.bson.codecs.Codec[T] {
  val getEncoderClass = classTag[T].runtimeClass.asInstanceOf[Class[T]]
  def encode(w: BsonWriter, value: T): Unit
  def decode(r: BsonReader): T
  def encode(writer: BsonWriter, value: T, encoderContext: EncoderContext) =
    encode(writer, value)
  def decode(reader: BsonReader, decoderContext: DecoderContext): T =
    decode(reader)
}
