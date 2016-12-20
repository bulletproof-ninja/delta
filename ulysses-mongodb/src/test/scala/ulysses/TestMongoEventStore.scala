package ulysses

import com.mongodb.client.result.DeleteResult
import com.mongodb.connection.ConnectionPoolSettings
import ulysses.mongo.{ StringCodec, UnitCodec }
import org.junit._, Assert._
import ulysses.util._
import ulysses.ddd._
import ulysses.testing._
import scuff.concurrent.Threads

object TestMongoEventStore {
  import com.mongodb.async.client._
  import org.bson.Document
  import ulysses.mongo._

  @volatile var coll: MongoCollection[Document] = _
  @volatile private var client: MongoClient = _

  @BeforeClass
  def setupClass {
    val poolSettings = ConnectionPoolSettings.builder.maxWaitQueueSize(200).maxSize(3).build().ensuring(_ != null)
    val settings = MongoClientSettings.builder.connectionPoolSettings(poolSettings).build().ensuring(_ != null)
    client = MongoClients.create(settings)
    coll = client.getDatabase("test").getCollection(getClass.getName)
  }
  @AfterClass
  def teardownClass {
    withBlockingCallback[Void]()(coll.drop(_))
    client.close()
  }
}

//@Ignore
class TestMongoEventStore extends AbstractEventStoreRepositoryTest {
  import TestMongoEventStore._
  import com.mongodb.async.client._
  import org.bson.Document
  import ulysses.mongo._

  implicit object MongoDBAggrEventCtx
      extends ReflectiveDecoder[AggrEvent, Document]
      with EventCodec[AggrEvent, Document]
      with AggrEventHandler {
    type RT = Document
    def version(evt: EventClass) = scuff.serialVersionUID(evt).toByte
    def name(evt: EventClass) = evt.getSimpleName

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
  def setup {
    val result = deleteAll()
    assertTrue(result.wasAcknowledged)
    es = new MongoEventStore[String, AggrEvent, Unit](coll) with LocalPublishing[String, AggrEvent, Unit] {
      def publishCtx = Threads.DefaultScheduler
    }
    repo = new EntityRepository(Threads.DefaultScheduler, SystemClock, TheOneAggr)(es)
  }
  private def deleteAll(): DeleteResult = {
    val result = withBlockingCallback[DeleteResult]() { callback =>
      coll.deleteMany(new Document, callback)
    }
    result.get
  }
  @After
  def teardown {
    val result = deleteAll()
    assertTrue(result.wasAcknowledged)
  }
}
