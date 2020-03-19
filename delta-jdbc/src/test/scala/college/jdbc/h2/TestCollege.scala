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
import delta.util.LocalTransport
import delta.testing.RandomDelayExecutionContext
import scala.util.Random
import scuff.jdbc.DataSourceConnection
import delta.MessageTransportPublishing
import scuff.jdbc.ConnectionSource
import college.CollegeEventFormat
import college.jdbc.StudentEmailsStore

object TestCollege {
  implicit object StringColumn extends VarCharColumn
  implicit object ByteArrayColumn extends VarBinaryColumn()

  val h2Name = s"delete-me.h2db.${Random.nextInt().abs}"
  val h2File = new File(".", h2Name + ".mv.db")
  @AfterClass
  def cleanup(): Unit = {
    h2File.delete()
  }
}

class TestCollege extends college.jdbc.TestCollege {

  import TestCollege._

  lazy val connSource = new ConnectionSource with DataSourceConnection {
    val dataSource = new JdbcDataSource
    dataSource.setURL(s"jdbc:h2:./${h2Name}")
  }

  override def newLookupServiceProcStore =
    new StudentEmailsStore(connSource, 1, WithTimestamp("last_updated"), ec)
    .ensureTable()

  override def newEventStore: EventStore[Int, CollegeEvent] = {
    val sql = new H2Dialect[Int, CollegeEvent, Array[Byte]](None)
    new JdbcEventStore(CollegeEventFormat, sql, connSource, RandomDelayExecutionContext)(initTicker)
    with MessageTransportPublishing[Int, CollegeEvent] {
      def toTopic(ch: Channel) = Topic(s"transactions:$ch")
      def toTopic(tx: Transaction): Topic = toTopic(tx.channel)
      val txTransport = new LocalTransport[Transaction](toTopic, RandomDelayExecutionContext)
      val txChannels = Set(college.semester.Semester.channel, college.student.Student.channel)
      val txCodec = scuff.Codec.noop[Transaction]
    }.ensureSchema()
  }

  @Test
  def mock(): Unit = {
    assertTrue(true)
  }

}
