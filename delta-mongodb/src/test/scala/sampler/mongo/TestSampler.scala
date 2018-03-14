package sampler.mongo

import org.junit.Assert.assertTrue
import org.junit.Test

import com.mongodb.MongoNamespace

import delta.mongo.MongoEventStore
import delta.testing.RandomDelayExecutionContext
import sampler.aggr.DomainEvent
import delta.Publishing
import delta.util.LocalPublisher

class TestSampler extends sampler.TestSampler {

  override lazy val es = {
    import delta.mongo._
    import com.mongodb.async.client._
    val client = MongoClients.create()
    val ns = new MongoNamespace("unit-testing", "event-store")
    val txnCollection = MongoEventStore.getCollection(ns, client, AggrRootRegistry)
      implicit def aggrCodec = AggrRootRegistry.codec
      implicit def evtCdc = BsonDomainEventCodec
    new MongoEventStore[Int, DomainEvent, sampler.Aggr.Value](
      txnCollection) with Publishing[Int, DomainEvent, sampler.Aggr.Value] {
      val publisher = new LocalPublisher[Int, DomainEvent, sampler.Aggr.Value](RandomDelayExecutionContext)
    }
  }

  @Test
  def mock() {
    assertTrue(true)
  }
}
