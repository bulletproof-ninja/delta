package college.jdbc.h2

import java.io.File

import org.h2.jdbcx.JdbcDataSource
import org.junit.AfterClass
import org.junit.Assert.assertTrue
import org.junit.Test

import college.CollegeEvent
import delta.EventStore
import delta.jdbc._
import delta.jdbc.h2.H2Dialect
import delta.util.LocalHub
import delta.testing.RandomDelayExecutionContext
import scala.util.Random
import scuff.jdbc.DataSourceConnection
import delta.Publishing

object TestCollege {
  implicit object StringColumn extends VarCharColumn
  implicit object ByteArrayColumn extends VarBinaryColumn

  val h2Name = s"delete-me.h2db.${Random.nextInt().abs}"
  val h2File = new File(".", h2Name + ".mv.db")
  @AfterClass
  def cleanup(): Unit = {
    h2File.delete()
  }
}

class TestCollege extends college.TestCollege {

  import TestCollege._

  override lazy val eventStore: EventStore[Int, CollegeEvent] = {
    val sql = new H2Dialect[Int, CollegeEvent, Array[Byte]](None)
    val ds = new JdbcDataSource
    ds.setURL(s"jdbc:h2:./${h2Name}")
    new JdbcEventStore[Int, CollegeEvent, Array[Byte]](
      sql, RandomDelayExecutionContext) with Publishing[Int, CollegeEvent] with DataSourceConnection {
      def toNamespace(ch: Channel) = Namespace(s"transactions:$ch")
      def toNamespace(txn: TXN): Namespace = toNamespace(txn.channel)
      val txnHub = new LocalHub[TXN](toNamespace, RandomDelayExecutionContext)
      val txnChannels = Set(college.semester.Semester.channel, college.student.Student.channel)
      protected def dataSource = ds
    }.ensureSchema()
  }

  @Test
  def mock(): Unit = {
    assertTrue(true)
  }

}
