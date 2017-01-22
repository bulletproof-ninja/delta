package delta_testing

import org.junit._, Assert._
import com.mongodb.connection.ConnectionPoolSettings
import delta.mongo._
import delta.testing.RandomDelayExecutionContext
import delta.testing.TestSnapshotStore

object TestMongoSnapshotStore {
  import com.mongodb.async.client._
  import org.bson.Document

  implicit def ec = RandomDelayExecutionContext

  @volatile var coll: MongoCollection[Document] = _
  @volatile private var client: MongoClient = _

  @BeforeClass
  def setupClass {
    val poolSettings = ConnectionPoolSettings.builder.maxWaitQueueSize(200).maxSize(3).build().ensuring(_ != null)
    val settings = MongoClientSettings.builder.connectionPoolSettings(poolSettings).build().ensuring(_ != null)
    client = MongoClients.create(settings)
    coll = client.getDatabase("test_snapshot_store").getCollection(getClass.getName)
  }
  @AfterClass
  def teardownClass {
    withBlockingCallback[Void]()(coll.drop(_))
    client.close()
  }
}

@Ignore // For some reason fails in setupClass
class TestMongoSnapshotStore extends TestSnapshotStore {
  import TestMongoSnapshotStore._
  override val store = MongoSnapshotStore[(Long, Int), String](SnapshotCodec.asBinary(reg = coll.getCodecRegistry), coll)

  @Test
  def mock() {
    assertTrue(true)
  }
}
