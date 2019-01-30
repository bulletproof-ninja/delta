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
import delta.Publishing
import org.postgresql.ds.PGSimpleDataSource
import scuff.SysProps
import delta.jdbc.postgresql.PostgreSQLDialect
import delta.jdbc.postgresql.ByteaColumn

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
    new JdbcEventStore[Int, CollegeEvent, Array[Byte]](
      sql, RandomDelayExecutionContext) with Publishing[Int, CollegeEvent] with DataSourceConnection {
      def toNamespace(ch: Channel) = Namespace(s"trans:$ch")
      val txnHub = new LocalHub[TXN](t => toNamespace(t.channel), RandomDelayExecutionContext)
      val txnChannels = Set(college.semester.Semester.channel, college.student.Student.channel)
      protected def dataSource = ds
    }.ensureSchema()
  }

  @Test
  def mock(): Unit = {
    assertTrue(true)
  }

}
