package sampler.jdbc.h2

import java.io.File

import org.h2.jdbcx.JdbcDataSource

import sampler.{ JSON, JsonDomainEventFormat }
import sampler.aggr.DomainEvent

import delta.jdbc._
import delta.jdbc.h2._
import delta.LamportTicker

import scala.util.Random

import scuff.LamportClock
import scuff.jdbc._
import scuff.concurrent._

import scala.concurrent.ExecutionContext

final class TestSampler
extends sampler.TestSampler {

  val h2Name = s"delete-me.h2db.${Random.nextInt().abs}"
  val h2File = new File(".", h2Name + ".mv.db")
  implicit object StringColumn extends VarCharColumn

  override def afterAll(): Unit = {
    h2File.delete()
  }



  override lazy val es = {
    val sql = new H2Dialect[Int, DomainEvent, JSON](None)
    val cs = new AsyncConnectionSource with DataSourceConnection {

      override def updateContext: ExecutionContext = ec

      override def queryContext: ExecutionContext = ec

      val dataSource = new JdbcDataSource
      dataSource.setURL(s"jdbc:h2:./${h2Name}")
    }
    new JdbcEventStore(JsonDomainEventFormat, sql, cs) {
      lazy val ticker = {
        val lampClock = new LamportClock(maxTick.await getOrElse -1L)
        LamportTicker(lampClock)
      }
      def publishCtx = ec
      def subscribeGlobal[U](selector: StreamsSelector)(callback: Transaction => U) =
        subscribeLocal(selector)(callback)
    }.ensureSchema()
  }

}
