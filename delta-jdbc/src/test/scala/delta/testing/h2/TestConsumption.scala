package delta.testing.h2

import delta.{EventFormat, EventStore, Ticker, TransactionPublishing}
import delta.testing._
import delta.jdbc._

import scuff.jdbc._
import scuff.Codec

import java.io.File
import java.util.UUID

import scala.util.Random
import scala.reflect.ClassTag

import org.h2.jdbcx.JdbcDataSource

import org.junit._

object TestConsumption {

  val h2Name = s"delete-me.h2db.${Random.nextInt().abs}"
  val h2File = new File(".", h2Name + ".mv.db")

  val cs = new AsyncConnectionSource with DataSourceConnection {
    def updateContext = RandomDelayExecutionContext
    def queryContext = RandomDelayExecutionContext
    val dataSource = new JdbcDataSource
    dataSource.setURL(s"jdbc:h2:./${h2Name}")
  }


  @AfterClass
  def cleanup(): Unit = {
    h2File.delete()
  }

}

class TestConsumption
extends delta.testing.TestConsumption {

  import TestConsumption._
  implicit def uuidCol = UUIDCharColumn
  implicit def strCol = VarCharColumn(1024)

  override def newEventStore[EVT](
      channels: Set[delta.Channel], ticker: Ticker, evtFmt: EventFormat[EVT, String])
      : EventStore[UUID, EVT] with TransactionPublishing[UUID, EVT] = {
    new JdbcEventStore[UUID, EVT, String](evtFmt, new DefaultDialect, cs)(_ => ticker)
    with LocalTransactionPublishing[UUID, EVT] {
      protected def txChannels = channels
    }.ensureSchema()
  }

  override def newProcessStore[S <: AnyRef: ClassTag](name: String)(
      implicit codec: Codec[S, String]) = {
    implicit def stateColumn = strCol.adapt(codec)
    val tableName = name.replace('-', '_')
    new JdbcStreamProcessStore[UUID, S, S](Config(pkColumn = "identifier", table = tableName), cs)
      .ensureTable()
  }
}
