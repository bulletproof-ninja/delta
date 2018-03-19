package college.mongodb

import org.junit.Assert._
import org.junit._

import college._
import delta.EventStore
import delta.testing.RandomDelayExecutionContext
import org.junit.AfterClass
import delta.mongo._
import delta.EventCodecAdapter
import scuff.Codec
import org.bson.Document
import com.mongodb.MongoNamespace
import org.bson.types.Binary
import delta.util.LocalPublisher
import delta.Publishing

object TestCollege {
  import com.mongodb.async.client._
  import org.bson.Document

  @volatile var coll: MongoCollection[Document] = _
  @volatile private var client: MongoClient = _

  @BeforeClass
  def setupClass() {
    client = MongoClients.create()
    val ns = new MongoNamespace("unit-testing", getClass.getName.replaceAll("[\\.\\$]+", "_"))
    coll = MongoEventStore.getCollection(ns, client)

  }
  @AfterClass
  def teardownClass() {
    withBlockingCallback[Void]()(coll.drop(_))
    client.close()
  }
}

class TestCollege extends college.TestCollege {
  import TestCollege._

  @After
  def dropColl() {
    withBlockingCallback[Void]()(coll.drop(_))
  }

  val BinaryDocCodec = new Codec[Array[Byte], Document] {
    def encode(bytes: Array[Byte]) = new Document("bytes", bytes)
    def decode(doc: Document): Array[Byte] = doc.get("bytes") match {
      case bin: Binary => bin.getData
    }
  }

  implicit def EvtCodec = new EventCodecAdapter(BinaryDocCodec)

  override lazy val eventStore: EventStore[Int, CollegeEvent] = {
    new MongoEventStore[Int, CollegeEvent](coll) with Publishing[Int, CollegeEvent] {
      val publisher = new LocalPublisher[Int, CollegeEvent](RandomDelayExecutionContext)
    }
  }

  @Test
  def mock() {
    assertTrue(true)
  }

}
