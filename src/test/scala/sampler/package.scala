import sampler.aggr.{ Aggregate, DomainEvent, Id }
import scuff.serialVersionUID
import ulysses.EventCodec
import ulysses.util.ReflectiveDecoder
import language.implicitConversions
import ulysses.jdbc.ScalaEnumColumn
import org.bson.Document
import org.bson.codecs.EncoderContext
import org.bson.BsonWriter
import org.bson.codecs.DecoderContext
import org.bson.BsonReader

package object sampler {

  type JSON = String

  implicit def id2uuid(id: Id[_]) = id.uuid

  implicit object AggrRootEnum
    extends ScalaEnumColumn[Aggregate.Root](Aggregate)

  trait AbstractEventCodec[SF]
      extends EventCodec[DomainEvent, SF] {

    private[this] val eventNames = new ClassValue[String] {
      def computeValue(cls: Class[_]) = {
        val fullName = cls.getName
        val sepIdx = fullName.lastIndexOf('.', fullName.lastIndexOf('.') - 1)
        fullName.substring(sepIdx + 1)
      }
    }

    def name(cls: ClassEVT): String = eventNames.get(cls)
    def version(cls: ClassEVT): Short = serialVersionUID(cls).toShort

  }

  implicit object JsonDomainEventCodec
      extends ReflectiveDecoder[DomainEvent, JSON]
      with AbstractEventCodec[JSON]
      with aggr.emp.JsonCodec
      with aggr.dept.JsonCodec {

    override type RT = JSON

    def encode(evt: DomainEvent) = evt match {
      case evt: aggr.dept.DeptEvent => evt.dispatch(this)
      case evt: aggr.emp.EmpEvent => evt.dispatch(this)
    }
  }

  object RootBsonCodec extends org.bson.codecs.Codec[Aggregate.Root] {
    val getEncoderClass = classOf[Aggregate.Value].asInstanceOf[Class[Aggregate.Root]]
    def encode(writer: BsonWriter, aggr: Aggregate.Root, encoderContext: EncoderContext) {
      writer.writeString(aggr.toString)
    }
    def decode(reader: BsonReader, decoderContext: DecoderContext): Aggregate.Root = {
      Aggregate fromName reader.readString()
    }
  }

  implicit object BsonDomainEventCodec
      extends AbstractEventCodec[Document] {
      def encode(evt: DomainEvent) = Document.parse(JsonDomainEventCodec.encode(evt))
      def decode(name: String, version: Short, data: Document) =
        JsonDomainEventCodec.decode(name, version, data.toJson)
  }

}
