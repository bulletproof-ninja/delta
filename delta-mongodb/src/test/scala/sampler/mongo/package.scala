package sampler

import scuff.serialVersionUID
import delta.EventCodec
import delta.util.ReflectiveDecoder
import language.implicitConversions
import org.bson.Document
import org.bson.codecs.EncoderContext
import org.bson.BsonWriter
import org.bson.codecs.DecoderContext
import org.bson.BsonReader
import org.bson.codecs.Codec
import org.bson.codecs.configuration.CodecRegistry
import org.bson.codecs.configuration.CodecConfigurationException
import sampler.aggr.dept.DeptEvent
import sampler.aggr.emp.EmpEvent
import sampler.aggr.DomainEvent

package object mongo {

  import sampler.Aggr

  object AggrRootRegistry
      extends CodecRegistry {
    val codec: Codec[Aggr.Value] = new AggrRootCodec
    def get[T](cls: Class[T]): Codec[T] = {
      byClass.getOrElse(cls, throw new CodecConfigurationException("Cannot find codec for $cls")).asInstanceOf[Codec[T]]
    }
    private[this] val byClass: Map[Class[_], Codec[_]] = {
      val codecs = Aggr.values.toSeq.map { aggr =>
        new AggrRootCodec {
          override def getEncoderClass = aggr.getClass.asInstanceOf[Class[Aggr.Value]]
        }
      } :+ codec
      codecs.map(c => c.getEncoderClass -> c).toMap
    }
    private class AggrRootCodec extends Codec[Aggr.Value] {
      def encode(writer: BsonWriter, value: Aggr.Value, ctx: EncoderContext) =
        writer.writeString(value.toString)
      def decode(reader: BsonReader, ctx: DecoderContext): Aggr.Value =
        sampler.Aggr withName reader.readString()
      def getEncoderClass = classOf[Aggr.Value]
    }
  }

  object RootBsonCodec extends org.bson.codecs.Codec[Aggr.Value] {
    val getEncoderClass = classOf[Aggr.Value].asInstanceOf[Class[Aggr.Value]]
    def encode(writer: BsonWriter, aggr: Aggr.Value, encoderContext: EncoderContext) {
      writer.writeString(aggr.toString)
    }
    def decode(reader: BsonReader, decoderContext: DecoderContext): Aggr.Value = {
      sampler.Aggr withName reader.readString()
    }
  }

  object BsonDomainEventCodec
      extends AbstractEventCodec[Document] {
    def encode(evt: DomainEvent) = Document.parse(JsonDomainEventCodec.encode(evt))
    def decode(name: String, version: Byte, data: Document) =
      JsonDomainEventCodec.decode(name, version, data.toJson)
  }

}
