package delta.cassandra

import com.datastax.driver.core.{ Cluster, Session, SocketOptions }
import delta.util._
import scala.reflect.classTag
import scala.util.Try
import delta.write.EntityRepository
import delta._
import delta.testing._
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
    def aggrCreated(enc: Encoded): AggrCreated = enc.version {
      case 1 => AggrCreated((JsVal parse enc.data).asStr)
    }

    def on(evt: NewNumberWasAdded) = evt.n.toString
    def newNumberWasAdded(enc: Encoded): NewNumberWasAdded = enc.version {
      case 1 => NewNumberWasAdded(enc.data.toInt)
    }

    def on(evt: StatusChanged) = JsStr(evt.newStatus).toJson
    def statusChanged(enc: Encoded): StatusChanged = enc.version {
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

  override def beforeEach(): Unit = {
    session = Cluster.builder().withSocketOptions(new SocketOptions().setConnectTimeoutMillis(10000)).addContactPoints("localhost").build().connect()
    deleteAll(session)
    es = new CassandraEventStore[String, AggrEvent, String](
      ec, session, TableDescriptor, AggrEventFormat, ec) {

        def ticker = TestCassandraEventStoreRepository.this.ticker

        def subscribeGlobal[U](selector: StreamsSelector)(callback: Transaction => U) =
          subscribeLocal(selector)(callback)

      }.ensureTable()
    repo = new EntityRepository(TheOneAggr)(es, ec)
  }
  private def deleteAll(session: Session): Unit = {
    Try(session.execute(s"DROP TABLE $Keyspace.$Table;"))
  }

  override def afterEach(): Unit = {
    // deleteAll(session)
  }

}
