package blogging.tests.jdbc.postgres

import java.util.UUID

import scala.concurrent.ExecutionContext

import blogging.BloggingEvent

import delta.testing.postgresql.PostgresDataSource
import delta.jdbc._
import delta.jdbc.postgresql._

import scuff._
import scuff.jdbc._

import blogging.tests.jdbc.JdbcTestBlogging


class PostgresTestBlogging
extends JdbcTestBlogging
with PostgresDataSource {

  lazy val connSource = new AsyncConnectionSource with DataSourceConnection {
    def updateContext: ExecutionContext = ec
    def queryContext: ExecutionContext = ec
    val dataSource = postgresDataSource
  }

  implicit def JSONColumn: ColumnType[JSON] = TextColumn
  def ShortString = VarCharColumn(255)
  implicit def UUIDColumn = UUIDCharColumn
  def shortColumn = ShortColumn
  def byteColumn = ShortColumn adapt Codec[Byte, Short](_.toShort, _.toByte)

  def dialect(implicit version: Option[Short]) =
    new PostgreSQLDialect[UUID, BloggingEvent, JSON](schema)
    with TableNameOverride {
      def isVersioned = version.isDefined
    }

  def newProcessStore(name: String)(implicit version: Option[Short]): JdbcProcStore = {
    new JdbcProcStore(name, schema, version)
  }.ensureTable()

}
