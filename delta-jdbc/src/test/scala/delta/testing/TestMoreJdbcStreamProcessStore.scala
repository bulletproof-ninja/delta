package delta.testing

import delta.jdbc._, JdbcStreamProcessStore._

import delta.process._

import java.sql._
import scuff.EmailAddress
import java.{util => ju}
import scuff.jdbc.AsyncConnectionSource

object TestMoreJdbcStreamProcessStore {

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

  def EmailColumn = NotNull("contact_email") { contact: Contact => contact.email }
  def NumColumn = Nullable[Contact, Int]("contact_number") { case contact: Contact if contact.num > 0 => contact.num }

}

abstract class TestMoreJdbcStreamProcessStore
extends TestMoreStreamProcessStore {

  import TestMoreJdbcStreamProcessStore._

  abstract class ProcessStore(
    protected val connectionSource: AsyncConnectionSource,
    schema: String = null)
  extends JdbcStreamProcessStore[Long, Contact, Contact](
      Table(s"contacts_${ju.UUID.randomUUID.toString.replace("-", "")}", schema = Option(schema))
        withPrimaryKey "id"
        withVersion 1,
      Index(EmailColumn),
      Index(NumColumn))

}
