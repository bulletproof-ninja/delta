package delta.mongo

import scala.reflect.{ClassTag, classTag}
import org.bson._, codecs._

class JavaEnumCodec[E <: java.lang.Enum[E]]()(implicit tag: ClassTag[E]) extends Codec[E] {

  def this(cls: Class[E]) = this()(ClassTag.apply[E](cls))

    val getEncoderClass = classTag[E].runtimeClass.asInstanceOf[Class[E]]
    private[this] val byName =
      getEncoderClass.getEnumConstants.foldLeft(Map.empty[String, E]) {
        case (map, enum: E) => map.updated(enum.name, enum)
        case _ => ???
      }
    def encode(writer: BsonWriter, value: E, encoderContext: EncoderContext): Unit = {
      stringCodec.encode(writer, value.name, encoderContext)
    }
    def decode(reader: BsonReader, decoderContext: DecoderContext): E = {
      byName apply stringCodec.decode(reader, decoderContext)
    }
  }
