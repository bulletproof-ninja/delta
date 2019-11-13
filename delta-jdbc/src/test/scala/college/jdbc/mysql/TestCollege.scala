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
import org.junit.AfterClass
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
  @AfterClass
  def dropDb(): Unit = {
    val conn = ds.getConnection
    try {
      val stm = conn.createStatement()
      try stm.execute(s"drop database if exists $db") finally stm.close()
    } finally conn.close()
  }

  implicit def DataColumn = BlobColumn
}

class TestCollege extends college.jdbc.TestCollege {

  import TestCollege._

  @Before
  def dropDb(): Unit = {
    TestCollege.dropDb()
    eventStore match {
      case es: JdbcEventStore[_, _, _] => es.ensureSchema()
    }
  }
  lazy val connSource = new ConnectionSource with DataSourceConnection {
    val dataSource = ds
  }

  override def newLookupServiceProcStore =
    (new StudentEmailsStore(connSource, 1, ec) with MySQLSyntax).ensureTable()

  override lazy val eventStore: EventStore[Int, CollegeEvent] = {
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
