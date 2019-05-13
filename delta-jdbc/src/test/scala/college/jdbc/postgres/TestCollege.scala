package college.jdbc.postgres

import org.junit.Assert._
import org.junit._

import college.CollegeEvent
import delta.EventStore
import delta.jdbc._
import delta.testing.RandomDelayExecutionContext
import delta.util.LocalHub
import org.junit.AfterClass
import scuff.jdbc.DataSourceConnection
import delta.MessageHubPublishing
import org.postgresql.ds.PGSimpleDataSource
import scuff.SysProps
import delta.jdbc.postgresql.PostgreSQLDialect
import delta.jdbc.postgresql.ByteaColumn
import scuff.jdbc.ConnectionSource
import college.CollegeEventFormat

object TestCollege {
  val schema = "delta_testing_college"
  val ds = {
    val ds = new PGSimpleDataSource
    ds.setUser("postgres")
    ds.setPassword(SysProps.required("delta.postgresql.password"))
    ds setUrl s"jdbc:postgresql://localhost/"
    ds
  }
  @AfterClass
  def dropDb(): Unit = {
    val conn = ds.getConnection
    try {
      val stm = conn.createStatement()
      try stm.execute(s"drop schema if exists $schema cascade") finally stm.close()
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
      implicit def DataColumn = ByteaColumn
    val sql = new PostgreSQLDialect[Int, CollegeEvent, Array[Byte]](schema)
    val cs = new ConnectionSource with DataSourceConnection {
      val dataSource = ds
    }
    new JdbcEventStore[Int, CollegeEvent, Array[Byte]](
      CollegeEventFormat,
      sql, cs, RandomDelayExecutionContext) with MessageHubPublishing[Int, CollegeEvent] {
      def toTopic(ch: Channel) = Topic(s"trans:$ch")
      val txnHub = new LocalHub[TXN](t => toTopic(t.channel), RandomDelayExecutionContext)
      val txnChannels = Set(college.semester.Semester.channel, college.student.Student.channel)
      val txnCodec = scuff.Codec.noop
    }.ensureSchema()
  }

  @Test
  def mock(): Unit = {
    assertTrue(true)
  }

}
