package delta_testing

import org.junit._, Assert._
import delta.mongo._
import delta.testing.RandomDelayExecutionContext
import delta.testing._
import org.bson._
import org.bson.codecs._

object TestMongoStreamProcessStore {
  import com.mongodb.async.client._

  implicit val TestKeyCodec = new Codec[TestKey] {
    def getEncoderClass = classOf[TestKey].asInstanceOf[Class[TestKey]]
    def encode(writer: BsonWriter, value: TestKey, encoderContext: EncoderContext): Unit = {
      writer.writeStartArray()
      writer.writeInt64(value.long)
      writer.writeInt32(value.int)
      writer.writeEndArray()
    }
    def decode(reader: BsonReader, decoderContext: DecoderContext): TestKey = {
      reader.readStartArray()
      val l = reader.readInt64()
      val i = reader.readInt32()
      reader.readEndArray()
      TestKey(l, i)
    }
  }

  implicit val ec = RandomDelayExecutionContext

  val snapshotVersion = 1: Short
  @volatile var coll: MongoCollection[BsonDocument] = _
  @volatile private var client: MongoClient = _

  @BeforeClass
  def setupClass(): Unit = {
    client = MongoClients.create()
    coll =
      client
      .getDatabase("test_snapshot_store")
      .getCollection(getClass.getName.replaceAll("[\\.\\$]+", "_") + "_" + snapshotVersion)
      .withDocumentClass(classOf[BsonDocument])
    withBlockingCallback[Void]()(coll.drop(_))
  }
  @AfterClass
  def teardownClass(): Unit = {
    withBlockingCallback[Void]()(coll.drop(_))
    client.close()
  }
}

class TestMongoStreamProcessStore extends TestStreamProcessStore {

  import TestMongoStreamProcessStore._
  implicit def exeCtx = ec
  implicit def snapshotCodec =
    SnapshotCodec[String]("string") {
      scuff.Codec(
        new BsonString(_),
        _.asString.getValue)
    }

  override def newStore() =
    new MongoStreamProcessStore[Long, String, String](coll)

  @Test
  def mock(): Unit = {
    assertTrue(true)
  }
}
