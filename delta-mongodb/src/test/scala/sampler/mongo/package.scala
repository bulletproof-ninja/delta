package sampler

import org.bson.BsonValue
import delta.mongo.BsonJsonCodec
import sampler.aggr.DomainEvent

package object mongo {

  object BsonDomainEventFormat
    extends AbstractEventFormat[BsonValue] {
    def encode(evt: DomainEvent) = BsonJsonCodec decode JsonDomainEventFormat.encode(evt)
    def decode(encoded: Encoded) = {
      JsonDomainEventFormat decode encoded.map(BsonJsonCodec.encode)
    }
  }

}
