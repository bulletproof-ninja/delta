package college.jdbc.postgres

import college._
import college.jdbc.JdbcEmailValidationProcessStore

import delta.jdbc.JdbcEventStore
import delta.validation.ConsistencyValidation
import delta.jdbc.postgresql._
import delta._

import scuff.SysProps
import scuff.jdbc._

import org.postgresql.ds.PGSimpleDataSource

import scala.concurrent.ExecutionContext

import scala.util.Random
import scuff.Codec

object TestCollege {
  val schema = s"delta_testing_${Random.nextInt().abs.toInt}"
  val ds = {
    val ds = new PGSimpleDataSource
    ds.setUser("postgres")
    ds.setPassword(SysProps.required("delta.postgresql.password"))
    ds setUrl s"jdbc:postgresql://localhost/"
    ds
  }

}

class TestCollege extends college.jdbc.TestCollege {

  import TestCollege._

  val connSource = new AsyncConnectionSource with DataSourceConnection {

    override def updateContext: ExecutionContext = ec

    override def queryContext: ExecutionContext = ec

    val dataSource = ds
  }

  override def newEmailValidationProcessStore() = {
    new JdbcEmailValidationProcessStore(connSource, 1, Some(schema), UpdateTimestamp("last_updated"))
  }.ensureTable()

  override def newEventStore[T](
      allChannels: Set[Channel],
      msgTransport: MessageTransport[T])(
      implicit
      encode: Transaction => T,
      decode: T => Transaction): CollegeEventStore = {
    implicit def Blob = ByteaColumn
    val sql = new PostgreSQLDialect[Int, CollegeEvent, Array[Byte]](schema)
    new JdbcEventStore(CollegeEventFormat, sql, connSource)
    with ConsistencyValidation[Int, CollegeEvent] {
      lazy val ticker = LamportTicker.subscribeTo(this)
      protected type TransportType = T
      def transport = msgTransport
      def subscribeGlobal[U](selector: StreamsSelector)(callback: Transaction => U) =
        subscribeLocal(selector)(callback)
      def txTransportChannels = allChannels
      val txTransportCodec = Codec(encode, decode)
      def publishCtx = ec
      // def toTopic(ch: Channel) = Topic(s"trans:$ch")
      // val txTransport = new LocalTransport[Transaction](ec)
      // val txChannels = Set(college.semester.Semester.channel, college.student.Student.channel)
      // val txTransportCodec = scuff.Codec.noop[Transaction]
      def validationContext(stream: Int) = ec
    }.ensureSchema()
  }

}
