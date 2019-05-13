package delta_testing

import org.junit._, Assert._
import delta.mongo._
import delta.testing.RandomDelayExecutionContext
import delta.testing._
import org.bson.BsonWriter
import org.bson.codecs._
import org.bson.BsonReader
import org.bson.Document

object TestMongoStreamProcessStore {
  import com.mongodb.async.client._
  import org.bson.Document

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
  @volatile var coll: MongoCollection[Document] = _
  @volatile private var client: MongoClient = _

  @BeforeClass
  def setupClass(): Unit = {
    client = MongoClients.create()
    coll =
      client
      .getDatabase("test_snapshot_store")
      .getCollection(getClass.getName.replaceAll("[\\.\\$]+", "_") + "_" + snapshotVersion)
    withBlockingCallback[Void]()(coll.drop(_))
  }
  @AfterClass
  def teardownClass(): Unit = {
    withBlockingCallback[Void]()(coll.drop(_))
    client.close()
  }
}

//@Ignore // For some reason fails in setupClass
class TestMongoStreamProcessStore extends TestStreamProcessStore {

  import TestMongoStreamProcessStore._
  implicit def exeCtx = ec
  val strCdc = new scuff.Codec[String, Document] {
    def encode(str: String) = new Document("string", str)
    def decode(doc: Document) = doc.getString("string")
  }
  override def newStore = new MongoStreamProcessStore[Long, String](strCdc, coll)

  @Test
  def mock(): Unit = {
    assertTrue(true)
  }
}
