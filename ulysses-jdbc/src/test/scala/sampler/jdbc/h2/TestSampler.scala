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
import ulysses.jdbc._
import ulysses.jdbc.h2.H2Dialect
import ulysses.util.LocalPublishing
import ulysses.testing.RandomDelayExecutionContext
import scala.util.Random

object TestSampler {
  val h2Name = s"delete-me.h2db.${Random.nextInt().abs}"
  val h2File = new File(".", h2Name + ".mv.db")
  @AfterClass
  def cleanup {
    h2File.delete()
  }
}

final class TestSampler extends sampler.TestSampler {

  override lazy val es = {
    val sql = new H2Dialect[Int, DomainEvent, Aggr.Value, JSON](None)
    val ds = new JdbcDataSource
    ds.setURL(s"jdbc:h2:./${TestSampler.h2Name}")
    new JdbcEventStore(ds, sql, RandomDelayExecutionContext) with LocalPublishing[Int, DomainEvent, Aggr.Value] {
      def publishCtx = RandomDelayExecutionContext
    }
  }

  @Test
  def mock {
    assertTrue(true)
  }
}
