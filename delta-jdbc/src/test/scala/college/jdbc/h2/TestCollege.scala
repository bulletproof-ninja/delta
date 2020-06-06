package college.jdbc.h2

import java.io.File

import org.h2.jdbcx.JdbcDataSource
import org.junit.AfterClass
import org.junit.Assert.assertTrue
import org.junit.Test

import college._
import delta.jdbc._
import delta.jdbc.h2.H2Dialect
import delta.util.LocalTransport
import delta.testing.RandomDelayExecutionContext
import scala.util.Random
import scuff.jdbc.DataSourceConnection
import delta.MessageTransportPublishing
import scuff.jdbc.AsyncConnectionSource
import college.CollegeEventFormat
import college.jdbc.JdbcEmailValidationProcessStore
import delta.validation.ConsistencyValidation
import scala.concurrent.ExecutionContext

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

  lazy val connSource = new AsyncConnectionSource with DataSourceConnection {

    override def updateContext: ExecutionContext = RandomDelayExecutionContext

    override def queryContext: ExecutionContext = RandomDelayExecutionContext

    val dataSource = new JdbcDataSource
    dataSource.setURL(s"jdbc:h2:./${h2Name}")
  }

  override def newEmailValidationProcessStore() =
    new JdbcEmailValidationProcessStore(connSource, 1, None, TimestampColumn("last_updated"))
    .ensureTable()

  override def newEventStore(): CollegeEventStore = {
    val sql = new H2Dialect[Int, CollegeEvent, Array[Byte]](None)
    new JdbcEventStore(CollegeEventFormat, sql, connSource)(initTicker)
    with MessageTransportPublishing[Int, CollegeEvent]
    with ConsistencyValidation[Int, CollegeEvent] {
      def toTopic(ch: Channel) = Topic(s"transactions:$ch")
      def toTopic(tx: Transaction): Topic = toTopic(tx.channel)
      val txTransport = new LocalTransport[Transaction](toTopic, RandomDelayExecutionContext)
      val txChannels = Set(college.semester.Semester.channel, college.student.Student.channel)
      val txCodec = scuff.Codec.noop[Transaction]
      def validationContext(stream: Int) = RandomDelayExecutionContext
    }.ensureSchema()
  }

  @Test
  def mock(): Unit = {
    assertTrue(true)
  }

}
