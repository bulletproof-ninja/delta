package college.jdbc.mysql

import java.sql.Connection

import org.junit.Assert.assertTrue
import org.junit._

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource

import college.CollegeEvent
import delta.EventStore
import delta.jdbc._
import delta.jdbc.mysql.MySQLDialect
import delta.testing.RandomDelayExecutionContext
import delta.util.LocalPublishing
import org.junit.AfterClass
import scuff.jdbc.DataSourceConnection

object TestCollege {
  val db = "delta_testing_college"
  val ds = {
    val ds = new MysqlDataSource
    ds.setUser("root")
    ds setUrl s"jdbc:mysql://localhost/$db?createDatabaseIfNotExist=true&useUnicode=true&characterEncoding=utf-8&autoReconnect=true"
    ds
  }
  @AfterClass
  def dropDb {
    val conn = ds.getConnection
    try {
      val stm = conn.createStatement()
      try stm.execute(s"drop database $db") finally stm.close()
    } finally conn.close()
  }
}

class TestCollege extends college.TestCollege {

  import TestCollege._

  override lazy val eventStore: EventStore[Int, CollegeEvent, String] = {
    implicit object ChannelColumn extends VarCharColumn(255)
    implicit def DataColumn = BlobColumn
    val sql = new MySQLDialect[Int, CollegeEvent, String, Array[Byte]]
    new JdbcEventStore[Int, CollegeEvent, String, Array[Byte]](
      sql, RandomDelayExecutionContext) with LocalPublishing[Int, CollegeEvent, String] with DataSourceConnection {
      protected def publishCtx = RandomDelayExecutionContext
      protected def dataSource = ds
    }.ensureSchema()
  }

  @Test
  def mock {
    assertTrue(true)
  }

}
