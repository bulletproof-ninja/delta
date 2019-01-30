package sampler.mongo

import com.mongodb.MongoNamespace

import delta.mongo.MongoEventStore
import delta.testing.RandomDelayExecutionContext
import sampler.aggr.DomainEvent
import delta.Publishing
import delta.util.LocalHub

import org.junit._, Assert._

class TestSampler extends sampler.TestSampler {

  override lazy val es = {
    import delta.mongo._
    val settings = com.mongodb.MongoClientSettings.builder().build()
    val ns = new MongoNamespace("unit-testing", "event-store")
    val txnCollection = MongoEventStore.getCollection(ns, settings)
      implicit def evtCdc = BsonDomainEventFormat
    new MongoEventStore[Int, DomainEvent](txnCollection) with Publishing[Int, DomainEvent] {
      def toNamespace(ch: Channel) = Namespace(ch.toString)
      val txnHub = new LocalHub[TXN](t => toNamespace(t.channel), RandomDelayExecutionContext)
      val txnChannels = Set(college.semester.Semester.channel, college.student.Student.channel)
    }
  }

  @Test
  def mock(): Unit = {
    assertTrue(true)
  }
}
