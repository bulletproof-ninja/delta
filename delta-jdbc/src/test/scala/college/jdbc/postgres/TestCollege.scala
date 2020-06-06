package college.jdbc.postgres

import org.junit.Assert._
import org.junit._

import college._
import college.jdbc.JdbcEmailValidationProcessStore

import delta.jdbc.JdbcEventStore
import delta.testing.RandomDelayExecutionContext
import delta.util.LocalTransport
import delta.MessageTransportPublishing
import delta.jdbc.postgresql._
import delta.validation.ConsistencyValidation

import scuff.SysProps
import scuff.jdbc._

import org.postgresql.ds.PGSimpleDataSource

import java.{util => ju}
import scala.concurrent.ExecutionContext

object TestCollege {
  val schema = s"delta_testing_${ju.UUID.randomUUID}".replace('-', '_')
  val ds = {
    val ds = new PGSimpleDataSource
    ds.setUser("postgres")
    ds.setPassword(SysProps.required("delta.postgresql.password"))
    ds setUrl s"jdbc:postgresql://localhost/"
    ds setCurrentSchema schema
    ds
  }

}

class TestCollege extends college.jdbc.TestCollege {

  import TestCollege._

  val connSource = new AsyncConnectionSource with DataSourceConnection {

    override def updateContext: ExecutionContext = RandomDelayExecutionContext

    override def queryContext: ExecutionContext = RandomDelayExecutionContext

    val dataSource = ds
  }

  override def newEmailValidationProcessStore() = {
    new JdbcEmailValidationProcessStore(connSource, 1, Some(schema), TimestampColumn("last_updated"))
  }.ensureTable()

  override def newEventStore(): CollegeEventStore = {
    implicit def Blob = ByteaColumn
    val sql = new PostgreSQLDialect[Int, CollegeEvent, Array[Byte]](schema)
    new JdbcEventStore(CollegeEventFormat, sql, connSource)(initTicker)
    with MessageTransportPublishing[Int, CollegeEvent]
    with ConsistencyValidation[Int, CollegeEvent] {
      def toTopic(ch: Channel) = Topic(s"trans:$ch")
      val txTransport = new LocalTransport[Transaction](t => toTopic(t.channel), RandomDelayExecutionContext)
      val txChannels = Set(college.semester.Semester.channel, college.student.Student.channel)
      val txCodec = scuff.Codec.noop[Transaction]
      def validationContext(stream: Int) = RandomDelayExecutionContext
    }.ensureSchema()
  }

  @Test
  def mock(): Unit = {
    assertTrue(true)
  }

}
