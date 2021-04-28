package blogging.tests.jdbc.h2

import java.io.File
import java.util.UUID

import scala.concurrent.ExecutionContext

import blogging.BloggingEvent

import delta.jdbc.{VarCharColumn, UUIDBinaryColumn, ShortColumn, ByteColumn, ColumnType}
import delta.jdbc.h2._

import org.h2.jdbcx.JdbcDataSource
import scuff.jdbc._
import java.sql.Connection

class H2TestBlogging
extends blogging.tests.jdbc.JdbcTestBlogging {

  val h2Name = s"delete-me.h2db-$instance"
  val h2File = new File(".", h2Name + ".mv.db")

  override def afterAll() = {
    h2File.delete
    super.afterAll()
  }

  lazy val connSource =
    new AsyncConnectionSource {
      private[this] val dataSource = {
        val ds = new JdbcDataSource
        ds.setURL(s"jdbc:h2:./${h2Name}")
        ds
      }
      protected def getConnection: Connection = {
        val conn = dataSource.getConnection
        conn setTransactionIsolation Connection.TRANSACTION_READ_COMMITTED
        conn
      }
      def updateContext: ExecutionContext = ec
      def queryContext: ExecutionContext = ec
    }

  implicit def JSONColumn: ColumnType[JSON] = VarCharColumn
  def ShortString = VarCharColumn
  implicit def UUIDColumn = UUIDBinaryColumn
  def shortColumn = ShortColumn
  def byteColumn = ByteColumn

  val schema = "blogging"

  def dialect(implicit version: Option[Short]) =
    new H2Dialect[UUID, BloggingEvent, JSON](schema)
    with TableNameOverride {
      def isVersioned = version.isDefined
    }

  def newProcessStore(name: String)(implicit version: Option[Short]): JdbcProcStore =
    new JdbcProcStore(name, schema, version).ensureTable()

}
