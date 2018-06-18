package delta_testing

import com.mongodb.client.result.DeleteResult

import org.junit._
import org.junit.Assert._
import delta.util._
import delta.ddd._
import delta.testing._
import delta.EventCodec
import delta.Publishing
import org.bson.BsonValue
import org.bson.BsonString
import org.bson.BsonInt32

object TestMongoEventStore {
  import com.mongodb.async.client._
  import org.bson.Document
  import delta.mongo._

  @volatile var coll: MongoCollection[Document] = _
  @volatile private var client: MongoClient = _

  @BeforeClass
  def setupClass(): Unit = {
    client = MongoClients.create()
    coll = client.getDatabase("test").getCollection(getClass.getName.replaceAll("[\\.\\$]+", "_"))
  }
  @AfterClass
  def teardownClass(): Unit = {
    withBlockingCallback[Void]()(coll.drop(_))
    client.close()
  }
}

class TestMongoEventStore extends AbstractEventStoreRepositoryTest {
  import TestMongoEventStore._
  import org.bson.Document
  import delta.mongo._

  implicit object MongoDBAggrEventCtx
      extends ReflectiveDecoder[AggrEvent, BsonValue]
      with EventCodec[AggrEvent, BsonValue]
      with AggrEventHandler {
    type Return = BsonValue

    def getVersion(evt: EventClass) = scuff.serialVersionUID(evt).toByte
    def getName(evt: EventClass) = evt.getSimpleName

    def encode(evt: AggrEvent): BsonValue = evt.dispatch(this)

    def on(evt: AggrCreated) = new BsonString(evt.status)
    def offAggrCreated(version: Byte, bson: BsonValue): AggrCreated = version match {
      case 1 => AggrCreated(bson.asString.getValue)
    }

    def on(evt: NewNumberWasAdded) = new BsonInt32(evt.n)
    def offNewNumberWasAdded(version: Byte, bson: BsonValue): NewNumberWasAdded = version match {
      case 1 => NewNumberWasAdded(bson.asNumber.asInt32.intValue)
    }

    def on(evt: StatusChanged) = new BsonString(evt.newStatus)
    def offStatusChanged(version: Byte, bson: BsonValue): StatusChanged = version match {
      case 1 => StatusChanged(bson.asString.getValue)
    }
  }

  @Before
  def setup(): Unit = {
    val result = deleteAll()
    assertTrue(result.wasAcknowledged)
    es = new MongoEventStore[String, AggrEvent](coll) with Publishing[String, AggrEvent] {
      val publisher = new LocalPublisher[String, AggrEvent](RandomDelayExecutionContext)
    }
    repo = new EntityRepository(TheOneAggr)(es)
  }
  private def deleteAll(): DeleteResult = {
    val result = withBlockingCallback[DeleteResult]() { callback =>
      coll.deleteMany(new Document, callback)
    }
    result.get
  }
  @After
  def teardown(): Unit = {
    val result = deleteAll()
    assertTrue(result.wasAcknowledged)
  }
}
