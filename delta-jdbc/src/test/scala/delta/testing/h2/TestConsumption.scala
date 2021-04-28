package delta.testing.h2

import delta.{ EventFormat, EventStore, Ticker }

import delta.jdbc._

import scuff.jdbc._
import scuff.Codec

import java.io.File
import java.util.UUID

import scala.util.Random
import scala.reflect.ClassTag

import org.h2.jdbcx.JdbcDataSource

import delta.jdbc.JdbcStreamProcessStore.Table

class TestConsumption
extends delta.testing.TestConsumption {


  val h2Name = s"delete-me.h2db.${Random.nextInt().abs}"
  val h2File = new File(".", h2Name + ".mv.db")

  val cs = new AsyncConnectionSource with DataSourceConnection {
    def updateContext = ec
    def queryContext = ec
    val dataSource = new JdbcDataSource
    dataSource.setURL(s"jdbc:h2:./${h2Name}")
  }

  override def afterAll(): Unit = {
    h2File.delete()
    super.afterAll()
  }

  implicit def uuidCol = UUIDCharColumn
  implicit def strCol = VarCharColumn(1024)

  override def newEventStore[EVT](
      theTicker: Ticker, evtFmt: EventFormat[EVT, String])
      : EventStore[UUID, EVT] = {
    new JdbcEventStore[UUID, EVT, String](evtFmt, new DefaultDialect, cs) {
      def ticker = theTicker
      def publishCtx = ec
      def subscribeGlobal[U](selector: StreamsSelector)(callback: Transaction => U) =
        subscribeLocal(selector)(callback)
    }.ensureSchema()
  }

  override def newProcessStore[S <: AnyRef: ClassTag](name: String)(
      implicit codec: Codec[S, String]) = {
    implicit def stateColumn = strCol.adapt(codec)
    val tableName = name.replace('-', '_')
    new JdbcStreamProcessStore[UUID, S, S](
      Table(tableName)
        withPrimaryKey "identifier") {
      def connectionSource = TestConsumption.this.cs
      def publishCtx = ec
    }
      .ensureTable()
  }
}
