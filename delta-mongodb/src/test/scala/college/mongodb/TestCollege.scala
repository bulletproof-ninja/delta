package college.mongodb

import org.junit._, Assert._

import college._
import college.validation.EmailValidationProcessStore
import college.validation.EmailValidationProcess.State

import delta.testing.RandomDelayExecutionContext
import delta.mongo._
import delta.util.LocalTransport
import delta.MessageTransportPublishing
import delta.validation.ConsistencyValidation

import org.bson._
import com.mongodb._

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._
import scala.collection.compat._

import scuff.EmailAddress


object TestCollege {
  import com.mongodb.async.client._
  import org.bson.Document

  final val EmailsField = "emails"

  implicit val bson2emails = (bson: BsonValue) => {
    bson.asArray
      .getValues.iterator.asScala
      .map(_.asString.getValue)
      .map(EmailAddress(_))
      .toSet
  }

  val emailsBsonCodec = new scuff.Codec[State, BsonValue] {
    def encode(state: State): BsonValue = {
      val lowerCased = state.asData.emails.toSeq.map(_.toLowerCase)
      new BsonArray(lowerCased.map(toBson).asJava)
    }
    def decode(b: BsonValue) = State(bson2emails(b))
  }


  implicit val emailStateCodec = new SnapshotCodec(emailsBsonCodec, EmailsField)

  var esColl: MongoCollection[Document] = _
  var emailColl: MongoCollection[BsonDocument] = _
  private var client: MongoClient = _

  @BeforeClass
  def setupClass(): Unit = {
    val settings = com.mongodb.MongoClientSettings.builder().build()
    client = MongoClients.create(settings)
    val ns = new MongoNamespace("unit-testing", getClass.getName.replaceAll("[\\.\\$]+", "_"))
    esColl = MongoEventStore.getCollection(ns, settings, client)
    emailColl = client
      .getDatabase(ns.getDatabaseName)
      .getCollection(ns.getCollectionName concat "_student-emails")
      .withDocumentClass(classOf[BsonDocument])

  }
  @AfterClass
  def teardownClass(): Unit = {
    withBlockingCallback[Void]()(esColl.drop(_))
    withBlockingCallback[Void]()(emailColl.drop(_))
    client.close()
  }
}

class TestCollege extends college.TestCollege {
  import TestCollege._

  @After
  def dropColl(): Unit = {
    withBlockingCallback[Void]()(esColl.drop(_))
    withBlockingCallback[Void]()(emailColl.drop(_))
  }

  def EvtFormat = CollegeEventFormat.adapt[BsonValue]

  override def newEmailValidationProcessStore(): EmailValidationProcessStore = {
    new MongoStreamProcessStore[Int, State, Unit](
      emailColl, EmailsField)
    with EmailValidationProcessStore {
      def emailRefName: String = EmailsField
      val emailRefType = (bson: BsonValue) => EmailAddress(bson.asString.getValue)
      def toQueryValue(addr: EmailAddress): BsonValue = new BsonString(addr.toLowerCase)
    }
  }.ensureIndexes()

  override def newEventStore(): CollegeEventStore = {
    new MongoEventStore[Int, CollegeEvent](esColl, EvtFormat)(initTicker)
    with MessageTransportPublishing[Int, CollegeEvent]
    with ConsistencyValidation[Int, CollegeEvent] {
      def toTopic(ch: Channel) = Topic(ch.toString)
      val txTransport = new LocalTransport[Transaction](t => toTopic(t.channel), RandomDelayExecutionContext)
      val txChannels = Set(college.semester.Semester.channel, college.student.Student.channel)
      val txCodec = scuff.Codec.noop[Transaction]
      def validationContext(stream: Int): ExecutionContext = RandomDelayExecutionContext
    }
  }.ensureIndexes()

  @Test
  def mock(): Unit = {
    assertTrue(true)
  }

}
