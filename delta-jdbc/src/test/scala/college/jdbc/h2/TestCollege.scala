package college.jdbc.h2

import java.io.File

import org.h2.jdbcx.JdbcDataSource

import delta._
import delta.jdbc._
import delta.jdbc.h2.H2Dialect
import delta.validation.ConsistencyValidation

import scala.util.Random
import scala.concurrent.ExecutionContext

import scuff.jdbc.DataSourceConnection
import scuff.jdbc.AsyncConnectionSource

import college.CollegeEventFormat
import college.jdbc.JdbcEmailValidationProcessStore
import college._

class TestCollege
extends college.jdbc.TestCollege {

  implicit object StringColumn extends VarCharColumn
  implicit object ByteArrayColumn extends VarBinaryColumn()

  val h2Name = s"delete-me.h2db.${Random.nextInt().abs}"
  val h2File = new File(".", h2Name + ".mv.db")

  override def afterAll() = {
    h2File.delete()
    super.afterAll()
  }

  lazy val connSource = new AsyncConnectionSource with DataSourceConnection {

    override def updateContext: ExecutionContext = ec

    override def queryContext: ExecutionContext = ec

    val dataSource = new JdbcDataSource
    dataSource.setURL(s"jdbc:h2:./${h2Name}")
  }

  override def newEmailValidationProcessStore() =
    new JdbcEmailValidationProcessStore(connSource, 1, None, UpdateTimestamp("last_updated"))
    .ensureTable()


  override def newEventStore[MT](
      allChannels: Set[Channel],
      txTransport: MessageTransport[MT])(
      implicit
      encode: Transaction => MT,
      decode: MT => Transaction)
      : college.CollegeEventStore = {

    val sql = new H2Dialect[Int, CollegeEvent, Array[Byte]](None)
    new JdbcEventStore(CollegeEventFormat, sql, connSource)
    with ConsistencyValidation[Int, CollegeEvent] {
      val ticker = LamportTicker(new scuff.LamportClock(0))
      def publishCtx = ec
      def subscribeGlobal[U](selector: StreamsSelector)(callback: Transaction => U) =
        subscribeLocal(selector)(callback)
      // def toTopic(ch: Channel) = Topic(s"transactions:$ch")
      // val txTransport = new LocalTransport[Transaction](ec)
      // val txChannels = Set(college.semester.Semester.channel, college.student.Student.channel)
      // val txTransportCodec = scuff.Codec.noop[Transaction]
      def validationContext(stream: Int) = ec
    }.ensureSchema()
  }

}
