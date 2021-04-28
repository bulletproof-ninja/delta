package blogging.tests.jdbc

import java.time._
import java.util.UUID

import scala.concurrent._

import blogging._
import blogging.tests._

import scuff._

import delta.cassandra._
import delta.testing.RandomDelayExecutionContext
import java.util.concurrent.Executors
import com.datastax.driver.core._
import scala.util.Try
import delta.Ticker
import delta.LamportTicker

object CassandraTestBlogging {

  implicit object WeekDayColumn
  extends ColumnType[DayOfWeek] {
    final val len = 3
    def stringValue(dow: DayOfWeek) = dow.name.substring(0, len)
    private[this] val lookup = DayOfWeek.values.map { dow =>
      stringValue(dow) -> dow
    }.toMap
    val cqlColumn = delta.cassandra.ASCIIColumn
    def typeName: String = cqlColumn.typeName
    override def writeAs(dow: DayOfWeek): AnyRef =
      cqlColumn writeAs stringValue(dow)
    def readFrom(rec: Rec, ref: Ref): DayOfWeek =
      lookup(cqlColumn.readFrom(rec, ref))
  }

}

class CassandraTestBlogging
extends blogging.tests.InMemTestBlogging {

  object TableDescriptor extends TableDescriptor {
    def keyspace = "blogging"
    def table = CassandraTestBlogging.this.getClass.getSimpleName
    val replication: Map[String, Any] = Map(
      "class" -> "SimpleStrategy",
      "replication_factor" -> 1)
  }

  override def newEventStore(clock: LamportClock)(implicit scenario: Unit): BloggingEventStore = {
    new CassandraEventStore[UUID, BloggingEvent, JSON](
      RandomDelayExecutionContext,
      session, TableDescriptor,
      JsonEventFormat,
      ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1), _.printStackTrace()))
    with BloggingEventStore {

      def subscribeGlobal[U](selector: StreamsSelector)(callback: Transaction => U): Subscription =
        subscribeLocal(selector)(callback)

      val ticker: Ticker = LamportTicker(clock)

    }

  }.ensureTable()


  var session: Session = _

  override def beforeEach(): Unit = {
    session = Cluster.builder()
      .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(10000))
      .addContactPoints("localhost")
      .build().connect()
    deleteAll(session)
  }
  private def deleteAll(session: Session): Unit = {
    import TableDescriptor._
    Try(session.execute(s"DROP TABLE $keyspace.$table;"))
  }

  override def afterEach(): Unit = {
    // deleteAll(session)
  }


}
