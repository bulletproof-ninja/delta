package college.mongodb

import org.junit.Assert._
import org.junit._

import college._
import delta.EventStore
import delta.testing.RandomDelayExecutionContext
import org.junit.AfterClass
import delta.mongo._
import delta.EventFormatAdapter
import scuff.Codec
import delta.util.LocalHub
import delta.Publishing
import org.bson.BsonValue
import com.mongodb._
import org.bson.BsonBinary

object TestCollege {
  import com.mongodb.async.client._
  import org.bson.Document

  @volatile var coll: MongoCollection[Document] = _
  @volatile private var client: MongoClient = _

  @BeforeClass
  def setupClass(): Unit = {
    val settings = com.mongodb.MongoClientSettings.builder().build()
    client = MongoClients.create(settings)
    val ns = new MongoNamespace("unit-testing", getClass.getName.replaceAll("[\\.\\$]+", "_"))
    coll = MongoEventStore.getCollection(ns, settings, client)

  }
  @AfterClass
  def teardownClass(): Unit = {
    withBlockingCallback[Void]()(coll.drop(_))
    client.close()
  }
}

class TestCollege extends college.TestCollege {
  import TestCollege._

  @After
  def dropColl(): Unit = {
    withBlockingCallback[Void]()(coll.drop(_))
  }

  val BinaryDocCodec = new Codec[Array[Byte], BsonValue] {
    def encode(bytes: Array[Byte]) = new BsonBinary(bytes)
    def decode(bson: BsonValue): Array[Byte] = bson.asBinary().getData
  }

  implicit def EvtCodec = new EventFormatAdapter(BinaryDocCodec)

  override lazy val eventStore: EventStore[Int, CollegeEvent] = {
    new MongoEventStore[Int, CollegeEvent](coll) with Publishing[Int, CollegeEvent] {
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
