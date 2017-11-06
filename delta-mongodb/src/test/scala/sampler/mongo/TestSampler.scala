package sampler.mongo

import org.junit.Assert.assertTrue
import org.junit.Test

import com.mongodb.MongoNamespace

import delta.mongo.MongoEventStore
import delta.testing.RandomDelayExecutionContext
import delta.util.LocalPublishing
import sampler.aggr.DomainEvent

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
      txnCollection) with LocalPublishing[Int, DomainEvent, sampler.Aggr.Value] {
      def publishCtx = RandomDelayExecutionContext
    }
  }

  @Test
  def mock() {
    assertTrue(true)
  }
}
