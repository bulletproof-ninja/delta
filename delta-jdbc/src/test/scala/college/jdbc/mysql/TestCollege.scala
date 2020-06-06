package college.jdbc.mysql

import org.junit.Assert._
import org.junit._

//import com.mysql.jdbc.jdbc2.optional.MysqlDataSource
import com.mysql.cj.jdbc.MysqlDataSource

import college._
import college.jdbc.JdbcEmailValidationProcessStore

import delta.jdbc._
import delta.jdbc.mysql._
import delta.testing.RandomDelayExecutionContext
import delta.util.LocalTransport
import delta.MessageTransportPublishing

import scuff.SysProps
import scuff.jdbc._

import java.{util => ju}

import delta.validation.ConsistencyValidation
import scala.concurrent.ExecutionContext

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

  lazy val connSource = new AsyncConnectionSource with DataSourceConnection {

    override def updateContext: ExecutionContext = RandomDelayExecutionContext

    override def queryContext: ExecutionContext = RandomDelayExecutionContext

    val dataSource = ds
  }

  override def newEmailValidationProcessStore() = {
    new JdbcEmailValidationProcessStore(connSource, 1, None, TimestampColumn("last_updated")) with MySQLSyntax
  }.ensureTable()

  override def newEventStore(): CollegeEventStore = {
    val sql = new MySQLDialect[Int, CollegeEvent, Array[Byte]]
    new JdbcEventStore(CollegeEventFormat, sql, connSource)(initTicker)
    with MessageTransportPublishing[Int, CollegeEvent]
    with ConsistencyValidation[Int, CollegeEvent] {
      def toTopic(ch: Channel) = Topic(ch.toString)
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
