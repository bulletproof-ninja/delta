package ulysses.cassandra

import org.junit._, Assert._

import com.datastax.driver.core.{ Cluster, Session, SocketOptions }
import ulysses.util.ReflectiveDecoder
import scala.reflect.{ ClassTag, classTag }
import concurrent.ExecutionContext.global
import ulysses.util.LocalPublishing
import ulysses.SystemClock
import scala.util.Try
import ulysses.ddd.EntityRepository
import ulysses._

@Ignore
class TestCassandraEventStoreRepository extends AbstractEventStoreRepositoryTest {

  implicit object AggrEventCodec
      extends ReflectiveDecoder[AggrEvent, String]
      with EventCodec[AggrEvent, String]
      with AggrEventHandler {
    type RT = String
    def name(cls: EventClass) = cls.getSimpleName
    def version(cls: EventClass) = scuff.serialVersionUID(cls).toByte

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

  val Keyspace = s"${getClass.getPackage.getName}"
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
  def setup {
    session = Cluster.builder().withSocketOptions(new SocketOptions().setConnectTimeoutMillis(10000)).addContactPoints("localhost").build().connect()
    deleteAll(session)
    es = new CassandraEventStore[String, AggrEvent, Unit, String](
      global, session, TableDescriptor) with LocalPublishing[String, AggrEvent, Unit] {
      def publishCtx = global
    }
    repo = new EntityRepository(global, SystemClock, TheOneAggr)(es)
  }
  private def deleteAll(session: Session): Unit = {
    Try(session.execute(s"DROP TABLE $Keyspace.$Table;"))
  }
  @After
  def teardown {
    val result = deleteAll(session)
  }
}
