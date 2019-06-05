package delta.cassandra

import org.junit._

import com.datastax.driver.core.{ Cluster, Session, SocketOptions }
import delta.util._
import scala.reflect.classTag
import scala.util.Try
import delta.ddd.EntityRepository
import delta._
import delta.testing._
import delta.Transaction.Channel
import scuff.Codec

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

    def on(evt: AggrCreated) = s"""{"status":"${evt.status}"}"""
    val FindStatus = """\{"status":"(\w+)"\}""".r
    def aggrCreated(enc: Encoded): AggrCreated = enc.version match {
      case 1 => FindStatus.findFirstMatchIn(enc.data).map(m => AggrCreated(m group 1)).getOrElse(throw new IllegalArgumentException(enc.data))
    }

    def on(evt: NewNumberWasAdded) = s"""{"number":${evt.n}}"""
    val FindNumber = """\{"number":(\d+)\}""".r
    def newNumberWasAdded(enc: Encoded): NewNumberWasAdded = enc.version match {
      case 1 => FindNumber.findFirstMatchIn(enc.data).map(m => NewNumberWasAdded(m.group(1).toInt)).getOrElse(throw new IllegalArgumentException(enc.data))
    }

    def on(evt: StatusChanged) = s"""{"newStatus":"${evt.newStatus}"}"""
    val FindNewStatus = """\{"newStatus":"(\w*)"\}""".r
    def statusChanged(enc: Encoded): StatusChanged = enc.version match {
      case 1 => FindNewStatus.findFirstMatchIn(enc.data).map(m => StatusChanged(m group 1)).getOrElse(throw new IllegalArgumentException(enc.data))
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
    es = new CassandraEventStore[String, AggrEvent, String](
      session, TableDescriptor, AggrEventFormat, RandomDelayExecutionContext) with MessageHubPublishing[String, AggrEvent] {
      def toTopic(ch: Channel) = Topic(s"txn:$ch")
      val txnHub = new LocalHub[TXN](t => toTopic(t.channel), RandomDelayExecutionContext)
      val txnChannels = Set(Channel("any"))
      val txnCodec = Codec.noop[TXN]
    }
    repo = new EntityRepository(TheOneAggr, ec)(es, ticker)
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
