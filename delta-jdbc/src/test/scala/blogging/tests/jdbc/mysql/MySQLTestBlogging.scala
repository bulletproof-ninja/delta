package blogging.tests.jdbc.mysql

import java.util.UUID

import blogging.BloggingEvent

import delta.jdbc._
import delta.jdbc.mysql._

import blogging.tests.jdbc.JdbcTestBlogging
import delta.testing.mysql.MySqlDataSource

class MySQLTestBlogging
extends JdbcTestBlogging
with MySqlDataSource {

  implicit def JSONColumn: ColumnType[JSON] = TextColumn64K
  def ShortString = VarCharColumn(255)
  implicit def UUIDColumn = UUIDCharColumn
  def shortColumn = ShortColumn
  def byteColumn = ByteColumn

  def dialect(implicit version: Option[Short]) =
    new MySQLDialect[UUID, BloggingEvent, JSON]
    with TableNameOverride {
      def isVersioned = version.isDefined
    }

  def newProcessStore(name: String)(implicit version: Option[Short]): JdbcProcStore = {
    new JdbcProcStore(name, None, version)
    with MySQLSyntax
  }.ensureTable()

}
