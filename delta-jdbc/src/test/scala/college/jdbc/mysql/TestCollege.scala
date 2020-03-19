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
import delta.util.LocalTransport
import scuff.jdbc.DataSourceConnection
import delta.MessageTransportPublishing
import scuff.jdbc.ConnectionSource
import college.CollegeEventFormat
import college.jdbc.StudentEmailsStore
import scuff.SysProps
import java.{util => ju}

object TestCollege {
  val db = s"delta_testing_${ju.UUID.randomUUID}".replace('-', '_')
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
    with MessageTransportPublishing[Int, CollegeEvent] {
      def toTopic(ch: Channel) = Topic(ch.toString)
      val txTransport = new LocalTransport[Transaction](t => toTopic(t.channel), RandomDelayExecutionContext)
      val txChannels = Set(college.semester.Semester.channel, college.student.Student.channel)
      val txCodec = scuff.Codec.noop[Transaction]
    }.ensureSchema()
  }

  @Test
  def mock(): Unit = {
    assertTrue(true)
  }

}
