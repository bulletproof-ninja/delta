package delta_testing

import com.mongodb.client.result.DeleteResult

import delta.util._
import delta.write._
import delta.testing._
import delta.EventFormat
import org.bson.BsonValue
import org.bson.BsonString
import org.bson.BsonInt32
import delta.LamportTicker
import scuff.LamportClock

class TestMongoEventStore extends AbstractEventStoreRepositoryTest {
  import com.mongodb.async.client._
  import org.bson.Document
  import delta.mongo._

  @volatile var coll: MongoCollection[Document] = _
  @volatile private var client: MongoClient = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    client = MongoClients.create()
    coll = client.getDatabase("test").getCollection(getClass.getName.replaceAll("[\\.\\$]+", "_"))
  }

  override def afterAll(): Unit = {
    super.afterAll()
    withBlockingCallback[Void]()(coll.drop(_))
    client.close()
  }

  object MongoDBAggrEventFmt
      extends ReflectiveDecoder[AggrEvent, BsonValue]
      with EventFormat[AggrEvent, BsonValue]
      with AggrEventHandler {
    type Return = BsonValue

    def getVersion(evt: EventClass) = scuff.serialVersionUID(evt).toByte
    def getName(evt: EventClass) = evt.getSimpleName

    def encode(evt: AggrEvent): BsonValue = evt.dispatch(this)

    def on(evt: AggrCreated) = new BsonString(evt.status)
    def offAggrCreated(bson: Encoded): AggrCreated = bson.version {
      case 1 => AggrCreated(bson.data.asString.getValue)
    }

    def on(evt: NewNumberWasAdded) = new BsonInt32(evt.n)
    def offNewNumberWasAdded(bson: Encoded): NewNumberWasAdded = bson.version {
      case 1 => NewNumberWasAdded(bson.data.asNumber.asInt32.intValue)
    }

    def on(evt: StatusChanged) = new BsonString(evt.newStatus)
    def offStatusChanged(bson: Encoded): StatusChanged = bson.version {
      case 1 => StatusChanged(bson.data.asString.getValue)
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    val result = deleteAll()
    assert(result.wasAcknowledged)
    es =
      new MongoEventStore[String, AggrEvent](
          coll, MongoDBAggrEventFmt) {
        lazy val ticker = LamportTicker.subscribeTo(this)
        def publishCtx = ec
        def subscribeGlobal[U](selector: StreamsSelector)(callback: Transaction => U) =
          subscribeLocal(selector)(callback)
        // type TransportType = Transaction
        // val transport = new LocalTransport[Transaction](ec)
        // val txTransportChannels = Set(college.semester.Semester.channel, college.student.Student.channel)
        // val txTransportCodec = scuff.Codec.noop[Transaction]
      }.ensureIndexes()
    repo = new EntityRepository(TheOneAggr)(es, ec)
  }
  private def deleteAll(): DeleteResult = {
    val result = withBlockingCallback[DeleteResult]() { callback =>
      coll.deleteMany(new Document, callback)
    }
    result.get
  }

  override def afterEach(): Unit = {
    val result = deleteAll()
    assert(result.wasAcknowledged)
    super.afterEach()
  }
}
