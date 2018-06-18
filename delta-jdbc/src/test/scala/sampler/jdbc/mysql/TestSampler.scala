package sampler.jdbc.mysql

import org.junit.Assert.assertTrue
import org.junit._

import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource

import sampler.{ JSON, JsonDomainEventCodec }
import sampler.aggr.DomainEvent
import delta.jdbc._
import delta.jdbc.mysql.MySQLDialect
import delta.testing.RandomDelayExecutionContext
import delta.util.LocalPublisher
import org.junit.AfterClass
import scuff.jdbc.DataSourceConnection
import delta.Publishing

object TestSampler {
  val db = "delta_testing_sampler"
  val ds = {
    val ds = new MysqlConnectionPoolDataSource
    ds.setUser("root")
    ds setUrl s"jdbc:mysql://localhost/$db?createDatabaseIfNotExist=true&useUnicode=true&characterEncoding=utf-8&autoReconnect=true"
    ds
  }
  @AfterClass
  def dropDb(): Unit = {
    val conn = ds.getConnection
    try {
      val stm = conn.createStatement()
      try stm.execute(s"drop database $db") finally stm.close()
    } finally conn.close()
  }
}

final class TestSampler extends sampler.TestSampler {

  implicit object JsonColumn extends VarCharColumn(Short.MaxValue)

  override lazy val es = {
    val sql = new MySQLDialect[Int, DomainEvent, JSON]
    new JdbcEventStore(sql, RandomDelayExecutionContext) with Publishing[Int, DomainEvent] with DataSourceConnection {
      val publisher = new LocalPublisher[Int, DomainEvent](RandomDelayExecutionContext)
      protected def dataSource = TestSampler.ds
    }.ensureSchema()
  }

  @Test
  def mock(): Unit = {
    assertTrue(true)
  }
}
