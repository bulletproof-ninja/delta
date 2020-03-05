package sampler.jdbc.mysql

import org.junit.Assert.assertTrue
import org.junit._

//import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource
import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource

import sampler.{ JSON, JsonDomainEventFormat }
import sampler.aggr.DomainEvent
import delta.jdbc._
import delta.jdbc.mysql.MySQLDialect
import delta.testing.RandomDelayExecutionContext
import delta.util.LocalHub
import org.junit.AfterClass
import scuff.jdbc.DataSourceConnection
import delta.MessageHubPublishing
import scuff.jdbc.ConnectionSource
import scuff.SysProps

object TestSampler {
  val db = "delta_testing_sampler"
  val ds = {
    val ds = new MysqlConnectionPoolDataSource
    ds.setUser("root")
    ds setPassword SysProps.required("delta.mysql.password")
    ds setUrl s"jdbc:mysql://localhost/$db?createDatabaseIfNotExist=true&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&useSSL=false"
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
    val cs = new ConnectionSource with DataSourceConnection {
      def dataSource = TestSampler.ds
    }
    new JdbcEventStore(JsonDomainEventFormat, sql, cs, RandomDelayExecutionContext)(initTicker)
    with MessageHubPublishing[Int, DomainEvent] {
      def toTopic(ch: Channel) = Topic(s"tx-$ch")
      def toTopic(tx: Transaction): Topic = toTopic(tx.channel)
      val txHub = new LocalHub[Transaction](toTopic, RandomDelayExecutionContext)
      val txChannels = Set(college.semester.Semester.channel, college.student.Student.channel)
      val txCodec = scuff.Codec.noop[Transaction]
    }.ensureSchema()
  }

  @Test
  def mock(): Unit = {
    assertTrue(true)
  }
}
