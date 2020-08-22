package delta.testing

import delta.jdbc._, JdbcStreamProcessStore._

import delta.process._

import java.sql._
import scuff.EmailAddress
import java.{util => ju}
import scuff.jdbc.AsyncConnectionSource

object TestMoreJdbcStreamProcessStore {

  implicit def ec = RandomDelayExecutionContext

  implicit object ContactColumn extends ColumnType[Contact] {
    private val native = VarCharColumn(255 + Int.MinValue.toString.length)
    def typeName: String = native.typeName
    override def writeAs(contact: Contact): AnyRef =
      native writeAs
        TestMoreStreamProcessStore.ContactCodec.encode(contact)
    def readFrom(rs: ResultSet, col: Int): Contact =
      TestMoreStreamProcessStore.ContactCodec decode
        native.readFrom(rs, col)
  }

  implicit object SearchEmailColumn extends ColumnType[EmailAddress] {
    private val native = VarCharColumn(255)
    def typeName: String = native.typeName
    override def writeAs(email: EmailAddress) = email.toLowerCase
    def readFrom(rs: ResultSet, col: Int) = EmailAddress(native.readFrom(rs, col))
  }

  def EmailColumn = Nullable("contact_email")((contact: Contact) => Option(contact.email))
  def NumColumn = NotNull("contact_number")((contact: Contact) => contact.num)

  val indexColumns =
    Index(EmailColumn) ::
    Index(NumColumn) ::
    Nil

}

abstract class TestMoreJdbcStreamProcessStore
extends TestMoreStreamProcessStore {

  import TestMoreJdbcStreamProcessStore._

  abstract class ProcessStore(cs: AsyncConnectionSource, schema: String = null)
  extends JdbcStreamProcessStore[Long, Contact, Contact](
      Config(
        pkColumn = "id",
        table = s"contacts_${ju.UUID.randomUUID.toString.replace("-", "")}")
        withVersion 1
        withSchema schema,
      cs,
      indexColumns)

}
