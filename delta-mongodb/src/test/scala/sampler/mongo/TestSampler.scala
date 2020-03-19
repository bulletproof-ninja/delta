package sampler.mongo

import com.mongodb.MongoNamespace

import delta.mongo.MongoEventStore
import delta.testing.RandomDelayExecutionContext
import sampler.aggr.DomainEvent
import delta.MessageTransportPublishing
import delta.util.LocalTransport

import org.junit._, Assert._

class TestSampler extends sampler.TestSampler {

  override lazy val es = {
    import delta.mongo._
    val settings = com.mongodb.MongoClientSettings.builder().build()
    val ns = new MongoNamespace("unit-testing", "event-store")
    val txCollection = MongoEventStore.getCollection(ns, settings)
    new MongoEventStore[Int, DomainEvent](txCollection, BsonDomainEventFormat)(initTicker)
    with MessageTransportPublishing[Int, DomainEvent] {
      def toTopic(ch: Channel) = Topic(ch.toString)
      val txTransport = new LocalTransport[Transaction](t => toTopic(t.channel), RandomDelayExecutionContext)
      val txChannels = Set(college.semester.Semester.channel, college.student.Student.channel)
      val txCodec = scuff.Codec.noop[Transaction]
    }
  }

  @Test
  def mock(): Unit = {
    assertTrue(true)
  }
}
