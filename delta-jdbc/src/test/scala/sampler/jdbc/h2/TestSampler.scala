package sampler.jdbc.h2

import java.io.File

import org.h2.jdbcx.JdbcDataSource
import org.junit.AfterClass
import org.junit.Assert.assertTrue
import org.junit.Test

import sampler.{ JSON, JsonDomainEventFormat }
import sampler.aggr.DomainEvent
import delta.jdbc._
import delta.jdbc.h2._
import delta.util.LocalHub
import delta.testing.RandomDelayExecutionContext
import scala.util.Random
import scuff.jdbc.DataSourceConnection
import delta.Publishing

object TestSampler {
  val h2Name = s"delete-me.h2db.${Random.nextInt().abs}"
  val h2File = new File(".", h2Name + ".mv.db")
  @AfterClass
  def cleanup(): Unit = {
    h2File.delete()
  }
  implicit object StringColumn extends VarCharColumn
}

final class TestSampler extends sampler.TestSampler {

  import TestSampler._

  override lazy val es = {
    val sql = new H2Dialect[Int, DomainEvent, JSON](None)
    val ds = new JdbcDataSource
    ds.setURL(s"jdbc:h2:./${h2Name}")
    new JdbcEventStore(sql, RandomDelayExecutionContext)
      with Publishing[Int, DomainEvent]
      with DataSourceConnection {
      def toNamespace(ch: Channel) = Namespace(ch.toString)
      val txnHub = new LocalHub[TXN](t => toNamespace(t.channel), RandomDelayExecutionContext)
      val txnChannels = Set(college.semester.Semester.channel, college.student.Student.channel)
      protected def dataSource = ds
    }.ensureSchema()
  }

  @Test
  def mock(): Unit = {
    assertTrue(true)
  }
}
