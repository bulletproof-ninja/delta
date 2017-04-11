package sampler.jdbc.mysql

import java.sql.Connection

import org.junit.Assert.assertTrue
import org.junit._

import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource

import sampler.{ Aggr, JSON, JsonDomainEventCodec }
import sampler.aggr.DomainEvent
import sampler.jdbc._
import delta.jdbc._
import delta.jdbc.mysql.MySQLDialect
import delta.testing.RandomDelayExecutionContext
import delta.util.LocalPublishing
import org.junit.AfterClass

object TestSampler {
  val db = "delta_testing_sampler"
  val ds = {
    val ds = new MysqlConnectionPoolDataSource
    ds.setUser("root")
    ds setUrl s"jdbc:mysql://localhost/$db?createDatabaseIfNotExist=true&useUnicode=true&characterEncoding=utf-8&autoReconnect=true"
    ds
  }
  @AfterClass
  def dropDb() {
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
    val sql = new MySQLDialect[Int, DomainEvent, Aggr.Value, JSON]
    new JdbcEventStore(sql, RandomDelayExecutionContext) with LocalPublishing[Int, DomainEvent, Aggr.Value] with DataSourceConnection {
      protected def publishCtx = RandomDelayExecutionContext
      protected def dataSource = TestSampler.ds
    }.ensureSchema()
  }

  @Test
  def mock() {
    assertTrue(true)
  }
}
