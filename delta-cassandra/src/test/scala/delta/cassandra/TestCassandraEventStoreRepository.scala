package delta.cassandra

import org.junit._

import com.datastax.driver.core.{ Cluster, Session, SocketOptions }
import delta.util._
import scala.reflect.classTag
import scala.util.Try
import delta.write.EntityRepository
import delta._
import delta.testing._
import delta.Transaction.Channel
import scuff.Codec
import scuff.json._, JsVal._

class TestCassandraEventStoreRepository extends delta.testing.AbstractEventStoreRepositoryTest {

  implicit object AggrEventFormat
      extends ReflectiveDecoder[AggrEvent, String]
      with EventFormat[AggrEvent, String]
      with AggrEventHandler {
    type Return = String

    def getName(cls: EventClass) = cls.getSimpleName
    def getVersion(cls: EventClass) = scuff.serialVersionUID(cls).toByte

    protected def evtTag = classTag[AggrEvent]
    protected def fmtTag = classTag[String]

    def encode(evt: AggrEvent): String = evt.dispatch(this)

    def on(evt: AggrCreated) = JsStr(evt.status).toJson
    def aggrCreated(enc: Encoded): AggrCreated = enc.version match {
      case 1 => AggrCreated((JsVal parse enc.data).asStr)
    }

    def on(evt: NewNumberWasAdded) = evt.n.toString
    def newNumberWasAdded(enc: Encoded): NewNumberWasAdded = enc.version match {
      case 1 => NewNumberWasAdded(enc.data.toInt)
    }

    def on(evt: StatusChanged) = JsStr(evt.newStatus).toJson
    def statusChanged(enc: Encoded): StatusChanged = enc.version match {
      case 1 => StatusChanged((JsVal parse enc.data).asStr)
    }
  }

  val Keyspace = s"${getClass.getPackage.getName}".replace(".", "_")
  val Table = getClass.getSimpleName

  object TableDescriptor extends TableDescriptor {
    def keyspace = Keyspace
    def table = Table
    val replication: Map[String, Any] = Map(
      "class" -> "SimpleStrategy",
      "replication_factor" -> 1)
  }

  var session: Session = _

  @Before
  def setup(): Unit = {
    session = Cluster.builder().withSocketOptions(new SocketOptions().setConnectTimeoutMillis(10000)).addContactPoints("localhost").build().connect()
    deleteAll(session)
    es = new CassandraEventStore[String, AggrEvent, String](session, TableDescriptor,
      AggrEventFormat, RandomDelayExecutionContext)(_ => ticker)
      with MessageTransportPublishing[String, AggrEvent] {
      def toTopic(ch: Channel) = Topic(s"tx:$ch")
      val txTransport = new LocalTransport[Transaction](t => toTopic(t.channel), RandomDelayExecutionContext)
      val txChannels = Set(Channel("any"))
      val txCodec = Codec.noop[Transaction]
    }.ensureTable()
    repo = new EntityRepository(TheOneAggr, ec)(es)
  }
  private def deleteAll(session: Session): Unit = {
    Try(session.execute(s"DROP TABLE $Keyspace.$Table;"))
  }
  @After
  def teardown(): Unit = {
    deleteAll(session)
  }

  @Test
  def mock(): Unit = {
    Assert.assertTrue(true)
  }
}
