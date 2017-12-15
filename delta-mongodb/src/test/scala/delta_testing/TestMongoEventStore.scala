package delta_testing

import com.mongodb.client.result.DeleteResult

import delta.mongo.{ stringCodec, unitCodec }
import org.junit._
import org.junit.Assert._
import delta.util._
import delta.ddd._
import delta.testing._
import scuff.concurrent.Threads
import delta.EventCodec

object TestMongoEventStore {
  import com.mongodb.async.client._
  import org.bson.Document
  import delta.mongo._

  @volatile var coll: MongoCollection[Document] = _
  @volatile private var client: MongoClient = _

  @BeforeClass
  def setupClass() {
    client = MongoClients.create()
    coll = client.getDatabase("test").getCollection(getClass.getName.replaceAll("[\\.\\$]+", "_"))
  }
  @AfterClass
  def teardownClass() {
    withBlockingCallback[Void]()(coll.drop(_))
    client.close()
  }
}

class TestMongoEventStore extends AbstractEventStoreRepositoryTest {
  import TestMongoEventStore._
  import org.bson.Document
  import delta.mongo._

  implicit object MongoDBAggrEventCtx
      extends ReflectiveDecoder[AggrEvent, Document]
      with EventCodec[AggrEvent, Document]
      with AggrEventHandler {
    type Return = Document

    def versionOf(evt: EventClass) = scuff.serialVersionUID(evt).toByte
    def nameOf(evt: EventClass) = evt.getSimpleName

    def encode(evt: AggrEvent): Document = evt.dispatch(this)

    def on(evt: AggrCreated) = new Document("status", evt.status)
    def offAggrCreated(version: Byte, doc: Document): AggrCreated = version match {
      case 1 => AggrCreated(doc.getString("status"))
    }

    def on(evt: NewNumberWasAdded) = new Document("number", evt.n)
    def offNewNumberWasAdded(version: Byte, doc: Document): NewNumberWasAdded = version match {
      case 1 => NewNumberWasAdded(doc.getInteger("number"))
    }

    def on(evt: StatusChanged) = new Document("newStatus", evt.newStatus)
    def offStatusChanged(version: Byte, doc: Document): StatusChanged = version match {
      case 1 => StatusChanged(doc.getString("newStatus"))
    }
  }

  @Before
  def setup() {
    val result = deleteAll()
    assertTrue(result.wasAcknowledged)
    es = new MongoEventStore[String, AggrEvent, Unit](coll) with LocalPublishing[String, AggrEvent, Unit] {
      def publishCtx = Threads.DefaultScheduler
    }
    repo = new EntityRepository((), TheOneAggr)(es)
  }
  private def deleteAll(): DeleteResult = {
    val result = withBlockingCallback[DeleteResult]() { callback =>
      coll.deleteMany(new Document, callback)
    }
    result.get
  }
  @After
  def teardown() {
    val result = deleteAll()
    assertTrue(result.wasAcknowledged)
  }
}
