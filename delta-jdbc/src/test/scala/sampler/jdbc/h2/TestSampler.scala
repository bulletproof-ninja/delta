package sampler.jdbc.h2

import java.io.File

import org.h2.jdbcx.JdbcDataSource
import org.junit.AfterClass
import org.junit.Assert.assertTrue
import org.junit.Test

import sampler.{ Aggr, JSON, JsonDomainEventCodec }
import sampler.aggr.DomainEvent
import sampler.jdbc.AggrRootColumn
import scuff.concurrent.Threads
import delta.jdbc._
import delta.jdbc.h2._
import delta.util.LocalPublishing
import delta.testing.RandomDelayExecutionContext
import scala.util.Random
import java.sql.Connection

object TestSampler {
  val h2Name = s"delete-me.h2db.${Random.nextInt().abs}"
  val h2File = new File(".", h2Name + ".mv.db")
  @AfterClass
  def cleanup {
    h2File.delete()
  }
  implicit object StringColumn extends VarCharColumn
}

final class TestSampler extends sampler.TestSampler {

  import TestSampler._

  override lazy val es = {
    val sql = new H2Dialect[Int, DomainEvent, Aggr.Value, JSON](None)
    val ds = new JdbcDataSource
    ds.setURL(s"jdbc:h2:./${h2Name}")
    new JdbcEventStore(sql, RandomDelayExecutionContext) with LocalPublishing[Int, DomainEvent, Aggr.Value] {
      def publishCtx = RandomDelayExecutionContext
      protected def useConnection[R](thunk: Connection => R): R = {
        val conn = ds.getConnection
        try thunk(conn) finally conn.close()
      }
    }
  }

  @Test
  def mock {
    assertTrue(true)
  }
}
