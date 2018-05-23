package sampler

import org.bson.BsonValue
import delta.mongo.BsonJsonCodec
import sampler.aggr.DomainEvent

package object mongo {

  object BsonDomainEventCodec
    extends AbstractEventCodec[BsonValue] {
    def encode(evt: DomainEvent) = BsonJsonCodec decode JsonDomainEventCodec.encode(evt)
    def decode(channel: String, name: String, version: Byte, data: BsonValue) =
      JsonDomainEventCodec.decode(channel, name, version, BsonJsonCodec encode data)
  }

}
