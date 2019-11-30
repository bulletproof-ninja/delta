package college.jdbc.mysql

import org.junit.Assert._
import org.junit._

//import com.mysql.jdbc.jdbc2.optional.MysqlDataSource
import com.mysql.cj.jdbc.MysqlDataSource

import college.CollegeEvent
import delta.EventStore
import delta.jdbc._
import delta.jdbc.mysql._
import delta.testing.RandomDelayExecutionContext
import delta.util.LocalHub
import scuff.jdbc.DataSourceConnection
import delta.MessageHubPublishing
import scuff.jdbc.ConnectionSource
import college.CollegeEventFormat
import college.jdbc.StudentEmailsStore
import scuff.SysProps

object TestCollege {
  val db = "delta_testing_college"
  val ds = {
    val ds = new MysqlDataSource
    ds setUser "root"
    ds setPassword SysProps.required("delta.mysql.password")
    ds setURL s"jdbc:mysql://localhost/$db"
    ds setCreateDatabaseIfNotExist true
    ds setUseSSL true
    ds setAutoReconnect true
    ds setCharacterEncoding "utf-8"
    ds setRewriteBatchedStatements false
    ds setContinueBatchOnError false
    ds
  }

  implicit def DataColumn = BlobColumn
}

class TestCollege extends college.jdbc.TestCollege {

  import TestCollege._

  lazy val connSource = new ConnectionSource with DataSourceConnection {
    val dataSource = ds
  }

  override def newLookupServiceProcStore =
    (new StudentEmailsStore(connSource, 1, WithTimestamp("last_updated"), ec) with MySQLSyntax).ensureTable()

  override def newEventStore: EventStore[Int, CollegeEvent] = {
    val sql = new MySQLDialect[Int, CollegeEvent, Array[Byte]]
    new JdbcEventStore(CollegeEventFormat, sql, connSource, RandomDelayExecutionContext)(initTicker)
    with MessageHubPublishing[Int, CollegeEvent] {
      def toTopic(ch: Channel) = Topic(ch.toString)
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
