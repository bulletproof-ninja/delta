package college.jdbc.postgres

import org.junit.Assert._
import org.junit._

import college.CollegeEvent
import delta.EventStore
import delta.jdbc.JdbcEventStore
import delta.testing.RandomDelayExecutionContext
import delta.util.LocalHub
import scuff.jdbc.DataSourceConnection
import delta.MessageHubPublishing
import org.postgresql.ds.PGSimpleDataSource
import scuff.SysProps
import delta.jdbc.postgresql._
import scuff.jdbc.ConnectionSource
import college.CollegeEventFormat
import college.jdbc.StudentEmailsStore

object TestCollege {
  val schema = "delta_testing_college"
  val ds = {
    val ds = new PGSimpleDataSource
    ds.setUser("postgres")
    ds.setPassword(SysProps.required("delta.postgresql.password"))
    ds setUrl s"jdbc:postgresql://localhost/"
    ds
  }

  implicit def Blob = ByteaColumn

}

class TestCollege extends college.jdbc.TestCollege {

  import TestCollege._

  val connSource = new ConnectionSource with DataSourceConnection {
    val dataSource = ds
  }

  override def newLookupServiceProcStore =
    (new StudentEmailsStore(connSource, 1, WithTimestamp("last_updated"), ec)).ensureTable()

  override def newEventStore: EventStore[Int, CollegeEvent] = {
    val sql = new PostgreSQLDialect[Int, CollegeEvent, Array[Byte]](schema)
    new JdbcEventStore(CollegeEventFormat, sql, connSource, RandomDelayExecutionContext)(initTicker)
    with MessageHubPublishing[Int, CollegeEvent] {
      def toTopic(ch: Channel) = Topic(s"trans:$ch")
      val txnHub = new LocalHub[TXN](t => toTopic(t.channel), RandomDelayExecutionContext)
      val txnChannels = Set(college.semester.Semester.channel, college.student.Student.channel)
      val txnCodec = scuff.Codec.noop[TXN]
    }.ensureSchema()
  }

  @Test
  def mock(): Unit = {
    assertTrue(true)
  }

}
