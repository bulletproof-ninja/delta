package sampler.jdbc.mysql

//import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource
import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource

import sampler.{ JSON, JsonDomainEventFormat }
import sampler.aggr.DomainEvent

import delta.LamportTicker
import delta.jdbc._
import delta.jdbc.mysql.MySQLDialect


import scuff.LamportClock
import scuff.jdbc._
import scuff.SysProps
import scuff.concurrent._

import scala.concurrent.ExecutionContext

final class TestSampler
extends sampler.TestSampler {

  val db = "delta_testing_sampler"
  val ds = {
    val ds = new MysqlConnectionPoolDataSource
    ds.setUser("root")
    ds setPassword SysProps.required("delta.mysql.password")
    ds setUrl s"jdbc:mysql://localhost/$db?createDatabaseIfNotExist=true&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&useSSL=false"
    ds
  }

  override def afterAll(): Unit = {
    val conn = ds.getConnection
    try {
      val stm = conn.createStatement()
      try stm.execute(s"drop database $db") finally stm.close()
    } finally conn.close()
    super.afterAll()
  }
  implicit object JsonColumn extends VarCharColumn(Short.MaxValue)

  override lazy val es = {
    val sql = new MySQLDialect[Int, DomainEvent, JSON]
    val cs = new AsyncConnectionSource with DataSourceConnection {
      override def updateContext: ExecutionContext = ec
      override def queryContext: ExecutionContext = ec
      def dataSource = ds
    }
    new JdbcEventStore(JsonDomainEventFormat, sql, cs) {
      lazy val ticker = LamportTicker(new LamportClock(maxTick.await getOrElse -1L))
      def publishCtx = ec
      def subscribeGlobal[U](selector: StreamsSelector)(callback: Transaction => U) =
        subscribeLocal(selector)(callback)
    }.ensureSchema()
  }

}
