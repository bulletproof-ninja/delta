package sampler.mongo

import com.mongodb.MongoNamespace

import sampler.aggr.DomainEvent
import delta.LamportTicker
import scuff.LamportClock
import scala.concurrent.ExecutionContext

class TestSampler extends sampler.TestSampler {

  override lazy val es = {
    import delta.mongo._
    val settings = com.mongodb.MongoClientSettings.builder().build()
    val ns = new MongoNamespace("unit-testing", "event-store")
    val txCollection = MongoEventStore.getCollection(ns, settings)
    new MongoEventStore[Int, DomainEvent](txCollection, BsonDomainEventFormat) {
      protected def publishCtx: ExecutionContext = ec
      lazy val ticker = LamportTicker(new LamportClock(0))
    }.ensureIndexes()
  }

}
