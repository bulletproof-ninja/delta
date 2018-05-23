package sampler.mongo

import com.mongodb.MongoNamespace

import delta.mongo.MongoEventStore
import delta.testing.RandomDelayExecutionContext
import sampler.aggr.DomainEvent
import delta.Publishing
import delta.util.LocalPublisher

import org.junit._, Assert._

class TestSampler extends sampler.TestSampler {

  override lazy val es = {
    import delta.mongo._
    val settings = com.mongodb.MongoClientSettings.builder().build()
    val ns = new MongoNamespace("unit-testing", "event-store")
    val txnCollection = MongoEventStore.getCollection(ns, settings)
      implicit def evtCdc = BsonDomainEventCodec
    new MongoEventStore[Int, DomainEvent](txnCollection) with Publishing[Int, DomainEvent] {
      val publisher = new LocalPublisher[Int, DomainEvent](RandomDelayExecutionContext)
    }
  }

  @Test
  def mock() {
    assertTrue(true)
  }
}
