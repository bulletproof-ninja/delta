package delta_testing

import delta.mongo._
import delta.testing._
import org.bson._
import org.bson.codecs._

object TestMongoStreamProcessStore {

}

class TestMongoStreamProcessStore extends TestStreamProcessStore {

  import com.mongodb.async.client._
  import TestStreamProcessStore._

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

  val snapshotVersion = 1: Short
  @volatile var coll: MongoCollection[BsonDocument] = _
  @volatile private var client: MongoClient = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    client = MongoClients.create()
    coll =
      client
      .getDatabase("test_snapshot_store")
      .getCollection(getClass.getName.replaceAll("[\\.\\$]+", "_") + "_" + snapshotVersion)
      .withDocumentClass(classOf[BsonDocument])
    withBlockingCallback[Void]()(coll.drop(_))
  }

  override def afterAll(): Unit = {
    withBlockingCallback[Void]()(coll.drop(_))
    client.close()
    super.afterAll()
  }


  implicit def snapshotCodec =
    SnapshotCodec[String]("string") {
      scuff.Codec(
        new BsonString(_),
        _.asString.getValue)
    }

  override def newStore() =
    new MongoStreamProcessStore[Long, String, String](coll)

}
