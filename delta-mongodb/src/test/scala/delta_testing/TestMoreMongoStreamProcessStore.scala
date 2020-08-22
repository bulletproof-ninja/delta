package delta_testing

import org.junit._, Assert._
import delta.mongo._
import delta.testing._
import org.bson._

import java.{util => ju}
import scuff.EmailAddress

import com.mongodb.async.client._

object TestMoreMongoStreamProcessStore {

  implicit val ec = RandomDelayExecutionContext

  @volatile private var client: MongoClient = _

  @BeforeClass
  def setupClass(): Unit = {
    client = MongoClients.create()
  }
  @AfterClass
  def teardownClass(): Unit = {
    client.close()
  }

  def ContactRef = "contact"
  def EmailField = "email_address"
  def NumField = "number"
  def SearchEmailField = "search_email"
  def SearchEmailPath = s"$ContactRef.$SearchEmailField"
  def SearchNumPath = s"$ContactRef.$NumField"

}

class TestMoreMongoStreamProcessStore
extends TestMoreStreamProcessStore {
  import TestMoreStreamProcessStore._
  import TestMoreMongoStreamProcessStore._

  private var coll: MongoCollection[BsonDocument] = _

  @Before
  def setup(): Unit = {
    val collName = s"contacts-${ju.UUID.randomUUID}"
    coll =
      client
      .getDatabase("test_stream_store")
      .getCollection(collName)
      .withDocumentClass(classOf[BsonDocument])
  }

  @After
  def teardown(): Unit = {
    withBlockingCallback[Void]()(coll.drop(_))
  }


  implicit def exeCtx = ec
  implicit def snapshotCodec =
    SnapshotCodec[Contact](ContactRef) {
      new scuff.Codec[Contact, BsonValue] {
        def encode(a: Contact): BsonValue =
          new BsonDocument(EmailField, a.email.toString)
            .append(SearchEmailField, a.email.toLowerCase)
            .append(NumField, a.num)
        def decode(b: BsonValue): Contact = {
          val doc = b.asDocument
          val email = scuff.EmailAddress(doc.getString(EmailField))
          val num = doc.getInt32(NumField)
          Contact(email, num)
        }
      }
    }

  class AbstractMongoStore
  extends MongoStreamProcessStore[Long, Contact, Contact](
    coll,
    SearchEmailPath, SearchNumPath)

  override def newLookupStore() = {
    new AbstractMongoStore with Lookup {
      def emailRef = SearchEmailPath
      def numRef = SearchNumPath
      def email2qry(email: EmailAddress): BsonValue = email.toLowerCase
      def num2qry(num: Int): BsonValue = num
    }
  }.ensureIndexes()

  override def newDupeStore() = {
    new AbstractMongoStore with DupeFinder {
      protected def emailRef: String = SearchEmailPath
      protected def getEmail: BsonValue => EmailAddress = str => EmailAddress(str.asString)
    }
  }.ensureIndexes()

  @Test
  def mock(): Unit = {
    assertTrue(true)
  }
}
