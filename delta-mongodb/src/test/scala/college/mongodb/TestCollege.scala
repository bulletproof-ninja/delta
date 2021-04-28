package college.mongodb

import college._
import college.validation.EmailValidationProcessStore
import college.validation.EmailValidationProcess.State

import delta.mongo._
import delta.util.LocalTransport
import delta.validation.ConsistencyValidation

import org.bson._
import com.mongodb._

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._
import scala.collection.compat._

import scuff.EmailAddress
import delta.{EventStore, MessageTransport}
import delta.Channel
import delta.LamportTicker

class TestCollege extends college.TestCollege {

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
      val lowerCased = state.asData.allEmails.toSeq.map(_.toLowerCase)
      new BsonArray(lowerCased.map(toBson).asJava)
    }
    def decode(b: BsonValue) = State(bson2emails(b))
  }


  implicit val emailStateCodec = new SnapshotCodec(emailsBsonCodec, EmailsField)

  var esColl: MongoCollection[Document] = _
  var emailColl: MongoCollection[BsonDocument] = _
  private var client: MongoClient = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val settings = com.mongodb.MongoClientSettings.builder().build()
    client = MongoClients.create(settings)
    val ns = new MongoNamespace("unit-testing", getClass.getName.replaceAll("[\\.\\$]+", "_"))
    esColl = MongoEventStore.getCollection(ns, settings, client)
    emailColl = client
      .getDatabase(ns.getDatabaseName)
      .getCollection(ns.getCollectionName concat "_student-emails")
      .withDocumentClass(classOf[BsonDocument])

  }
  override def afterAll(): Unit = {
    super.afterAll()
    withBlockingCallback[Void]()(esColl.drop(_))
    withBlockingCallback[Void]()(emailColl.drop(_))
    client.close()
  }

  override def afterEach(): Unit = {
    withBlockingCallback[Void]()(esColl.drop(_))
    withBlockingCallback[Void]()(emailColl.drop(_))
    super.afterEach()
  }

  def EvtFormat = CollegeEventFormat.adapt[BsonValue]

  override def newEmailValidationProcessStore(): EmailValidationProcessStore = {
    new MongoStreamProcessStore[Int, State, Unit](
      emailColl, EmailsField)
    with EmailValidationProcessStore {
      def emailRefName: String = EmailsField
      val getEmail = bson => EmailAddress(bson.asString.getValue)
      def toQueryValue(addr: EmailAddress): BsonValue = new BsonString(addr.toLowerCase)
    }
  }.ensureIndexes()

  override def newEventStore[MT](
      allChannels: Set[Channel.Type],
      txTransport: MessageTransport[MT])(
      implicit
      encode: Transaction => MT,
      decode: MT => Transaction) = {

    new MongoEventStore[Int, CollegeEvent](esColl, EvtFormat)
    with ConsistencyValidation[Int, CollegeEvent] {
      lazy val ticker = LamportTicker.subscribeTo(this)
      def validationContext(stream: Int): ExecutionContext = ec
      def publishCtx = ec
      def subscribeGlobal[U](selector: StreamsSelector)(callback: Transaction => U) =
        subscribeLocal(selector)(callback)
      // type TransportType = MT
      // val transport = txTransport
      // val txTransportChannels = Set(college.semester.Channel, college.student.Channel)
      // val txTransportCodec = scuff.Codec(encode, decode)
    }
  }.ensureIndexes()

}
