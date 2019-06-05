package college.jdbc.mysql

import org.junit.Assert._
import org.junit._

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource

import college.CollegeEvent
import delta.EventStore
import delta.jdbc._
import delta.jdbc.mysql.MySQLDialect
import delta.testing.RandomDelayExecutionContext
import delta.util.LocalHub
import org.junit.AfterClass
import scuff.jdbc.DataSourceConnection
import delta.MessageHubPublishing
import scuff.jdbc.ConnectionSource
import college.CollegeEventFormat

object TestCollege {
  val db = "delta_testing_college"
  val ds = {
    val ds = new MysqlDataSource
    ds setUser "root"
    ds setURL s"jdbc:mysql://localhost/$db"
    ds setCreateDatabaseIfNotExist true
    ds setUseUnicode true
    ds setUseSSL true
    ds setAutoReconnect true
    ds setCharacterEncoding "utf-8"
    ds setRewriteBatchedStatements true
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
}

class TestCollege extends college.TestCollege {

  @Before
  def dropDb(): Unit = {
    TestCollege.dropDb()
    eventStore match {
      case es: JdbcEventStore[_, _, _] => es.ensureSchema()
    }
  }

  import TestCollege._

  override lazy val eventStore: EventStore[Int, CollegeEvent] = {
    implicit def DataColumn = BlobColumn
    val sql = new MySQLDialect[Int, CollegeEvent, Array[Byte]]
    val cs = new ConnectionSource with DataSourceConnection {
      val dataSource = ds
    }
    new JdbcEventStore[Int, CollegeEvent, Array[Byte]](
      CollegeEventFormat,
      sql, cs, RandomDelayExecutionContext) with MessageHubPublishing[Int, CollegeEvent] {
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
