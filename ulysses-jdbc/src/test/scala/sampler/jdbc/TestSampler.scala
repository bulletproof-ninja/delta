package sampler.jdbc

import java.io.File
import java.sql.ResultSet

import scala.concurrent.{ Await, ExecutionContext }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{ Failure, Random, Success, Try }

import org.h2.jdbcx.JdbcDataSource
import org.junit.{ Before, Test }
import org.junit.AfterClass
import org.junit.Assert._

import sampler.aggr._
import scuff._
import scuff.ddd.Repository
import ulysses.{ EventStore, LamportClock }
import ulysses.ddd.EntityRepository
import ulysses.jdbc._
import ulysses.util.LocalPublishing
import scuff.ddd.DuplicateIdException
import scuff.concurrent.{
  StreamCallback,
  StreamPromise
}
import ulysses.EventSource
import scala.concurrent.Promise
import ulysses.EventCodec
import scuff.reflect.Surgeon
import sampler.JsonDomainEventCodec
import sampler.Aggr

object TestSampler {
  val h2Name = "h2db"
  val h2File = new File(".", h2Name + ".mv.db")
  @AfterClass
  def cleanup {
    h2File.delete()
  }
}

class TestSampler extends sampler.TestSampler {

  override val es = {
    val sql = new Dialect[Int, DomainEvent, Aggr.Value, JSON](None)
    val ds = new JdbcDataSource
    ds.setURL(s"jdbc:h2:./${TestSampler.h2Name}")
    new JdbcEventStore(ds, sql) with LocalPublishing[Int, DomainEvent, Aggr.Value] {
      def publishCtx = implicitly[ExecutionContext]
    }
  }

}
