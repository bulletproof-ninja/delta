package college.jdbc.mysql

import org.junit.Assert._
import org.junit._

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource

import college.CollegeEvent
import delta.EventStore
import delta.jdbc._
import delta.jdbc.mysql.MySQLDialect
import delta.testing.RandomDelayExecutionContext
import delta.util.LocalPublisher
import org.junit.AfterClass
import scuff.jdbc.DataSourceConnection
import delta.Publishing

object TestCollege {
  val db = "delta_testing_college"
  val ds = {
    val ds = new MysqlDataSource
    ds.setUser("root")
    ds setUrl s"jdbc:mysql://localhost/$db?createDatabaseIfNotExist=true&useUnicode=true&characterEncoding=utf-8&autoReconnect=true"
    ds
  }
  @AfterClass
  def dropDb() {
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
    new JdbcEventStore[Int, CollegeEvent, Array[Byte]](
      sql, RandomDelayExecutionContext) with Publishing[Int, CollegeEvent] with DataSourceConnection {
      val publisher = new LocalPublisher[Int, CollegeEvent](RandomDelayExecutionContext)
      protected def dataSource = ds
    }.ensureSchema()
  }

  @Test
  def mock() {
    assertTrue(true)
  }

}
