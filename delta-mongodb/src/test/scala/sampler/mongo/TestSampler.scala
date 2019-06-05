package sampler.mongo

import com.mongodb.MongoNamespace

import delta.mongo.MongoEventStore
import delta.testing.RandomDelayExecutionContext
import sampler.aggr.DomainEvent
import delta.MessageHubPublishing
import delta.util.LocalHub

import org.junit._, Assert._

class TestSampler extends sampler.TestSampler {

  override lazy val es = {
    import delta.mongo._
    val settings = com.mongodb.MongoClientSettings.builder().build()
    val ns = new MongoNamespace("unit-testing", "event-store")
    val txnCollection = MongoEventStore.getCollection(ns, settings)
    new MongoEventStore[Int, DomainEvent](txnCollection, BsonDomainEventFormat) with MessageHubPublishing[Int, DomainEvent] {
      def toTopic(ch: Channel) = Topic(ch.toString)
      val txnHub = new LocalHub[TXN](t => toTopic(t.channel), RandomDelayExecutionContext)
      val txnChannels = Set(college.semester.Semester.channel, college.student.Student.channel)
      val txnCodec = scuff.Codec.noop[TXN]
    }
  }

  @Test
  def mock(): Unit = {
    assertTrue(true)
  }
}
