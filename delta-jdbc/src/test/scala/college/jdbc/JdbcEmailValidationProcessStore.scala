package college.jdbc

import java.sql.ResultSet

import delta.jdbc._
import delta.process._

import scuff.jdbc.AsyncConnectionSource

import college.validation._, EmailValidationProcess.State
import scuff.EmailAddress

private object JdbcEmailValidationProcessStore {
  val EmailColumn = "email_address"
  implicit val EmailColumnType = VarCharColumn(255)
  implicit val StateColumnType = new ColumnType[State] {
    val underlying = VarCharColumn(255 * 64)
    def readFrom(rs: ResultSet, col: Int): State = State(underlying.readFrom(rs, col))
    override def writeAs(state: State): AnyRef = state.asJson.value
    def typeName: String = underlying.typeName

  }
}

import JdbcStreamProcessStore._
import JdbcEmailValidationProcessStore._

class JdbcEmailValidationProcessStore(
  protected val connectionSource: AsyncConnectionSource,
  version: Short, schema: Option[String],
  updateTimestamp: UpdateTimestamp)
extends JdbcStreamProcessStore[Int, State, Unit](
  Table("student_email_lookup", schema = schema)
    withPrimaryKey "student_id"
    withVersion version
    withTimestamp updateTimestamp)
with IndexTableSupport[Int, State, Unit]
with EmailValidationProcessStore {

  protected val indexTables =
    IndexTable[String](EmailColumn)(_.asData.allEmails.map(_.toLowerCase)) ::
    Nil

  override def toQueryValue(addr: EmailAddress) = addr.toLowerCase
  override def emailRefName: String = EmailColumn
  override val getEmail = new ReadColumn[EmailAddress] {
    def readFrom(rs: ResultSet, col: Int): EmailAddress = EmailAddress(rs.getString(col))
  }

}
