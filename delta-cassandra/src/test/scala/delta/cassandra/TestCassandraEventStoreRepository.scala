package delta.cassandra

import org.junit._

import com.datastax.driver.core.{ Cluster, Session, SocketOptions }
import delta.util._
import scala.reflect.classTag
import scala.util.Try
import delta.ddd.EntityRepository
import delta._
import delta.testing._

class TestCassandraEventStoreRepository extends delta.testing.AbstractEventStoreRepositoryTest {

  implicit object AggrEventCodec
      extends ReflectiveDecoder[AggrEvent, String]
      with EventCodec[AggrEvent, String]
      with AggrEventHandler {
    type Return = String

    def getName(cls: EventClass) = cls.getSimpleName
    def getVersion(cls: EventClass) = scuff.serialVersionUID(cls).toByte

    protected def evtTag = classTag[AggrEvent]
    protected def fmtTag = classTag[String]

    def encode(evt: AggrEvent): String = evt.dispatch(this)

    def on(evt: AggrCreated) = s"""{"status":"${evt.status}"}"""
    val FindStatus = """\{"status":"(\w+)"\}""".r
    def aggrCreated(version: Byte, json: String): AggrCreated = version match {
      case 1 => FindStatus.findFirstMatchIn(json).map(m => AggrCreated(m group 1)).getOrElse(throw new IllegalArgumentException(json))
    }

    def on(evt: NewNumberWasAdded) = s"""{"number":${evt.n}}"""
    val FindNumber = """\{"number":(\d+)\}""".r
    def newNumberWasAdded(version: Byte, json: String): NewNumberWasAdded = version match {
      case 1 => FindNumber.findFirstMatchIn(json).map(m => NewNumberWasAdded(m.group(1).toInt)).getOrElse(throw new IllegalArgumentException(json))
    }

    def on(evt: StatusChanged) = s"""{"newStatus":"${evt.newStatus}"}"""
    val FindNewStatus = """\{"newStatus":"(\w*)"\}""".r
    def statusChanged(version: Byte, json: String): StatusChanged = version match {
      case 1 => FindNewStatus.findFirstMatchIn(json).map(m => StatusChanged(m group 1)).getOrElse(throw new IllegalArgumentException(json))
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
  def setup() {
    session = Cluster.builder().withSocketOptions(new SocketOptions().setConnectTimeoutMillis(10000)).addContactPoints("localhost").build().connect()
    deleteAll(session)
    es = new CassandraEventStore[String, AggrEvent, String](
      session, TableDescriptor) with Publishing[String, AggrEvent] {
      val publisher = new LocalPublisher[String, AggrEvent](RandomDelayExecutionContext)
    }
    repo = new EntityRepository(TheOneAggr)(es)
  }
  private def deleteAll(session: Session): Unit = {
    Try(session.execute(s"DROP TABLE $Keyspace.$Table;"))
  }
  @After
  def teardown() {
    deleteAll(session)
  }

  @Test
  def mock() {
    Assert.assertTrue(true)
  }
}
