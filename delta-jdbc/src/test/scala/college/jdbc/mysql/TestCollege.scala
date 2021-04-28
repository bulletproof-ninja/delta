package college.jdbc.mysql

import college._
import college.jdbc.JdbcEmailValidationProcessStore

import delta.LamportTicker
import delta.jdbc._
import delta.jdbc.mysql._
import delta.validation.ConsistencyValidation
import delta.testing.mysql.MySqlDataSource

import scuff.LamportClock
import scuff.concurrent._

import college.CollegeEventStore
import delta.MessageTransport
import delta.Channel

class TestCollege
extends college.jdbc.TestCollege
with MySqlDataSource {

  implicit def DataColumn = BlobColumn

  override def newEmailValidationProcessStore() = {
    new JdbcEmailValidationProcessStore(connSource, 1, None, UpdateTimestamp("last_updated"))
    with MySQLSyntax
  }.ensureTable()

  override def newEventStore[MT](
      allChannels: Set[Channel], txTransport: MessageTransport[MT])(
      implicit
      encode: Transaction => MT,
      decode: MT => Transaction): CollegeEventStore = {
    val sql = new MySQLDialect[Int, CollegeEvent, Array[Byte]]
    new JdbcEventStore(CollegeEventFormat, sql, connSource)
    with ConsistencyValidation[Int, CollegeEvent] {
      lazy val ticker = LamportTicker {
        new LamportClock(maxTick.await getOrElse 0L)
      }
      def publishCtx = ec
      def subscribeGlobal[U](selector: StreamsSelector)(callback: Transaction => U) =
        subscribeLocal(selector)(callback)
      def validationContext(stream: Int) = ec
    }.ensureSchema()
  }

}
