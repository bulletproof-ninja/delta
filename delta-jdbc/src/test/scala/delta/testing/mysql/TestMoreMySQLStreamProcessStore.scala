package delta.testing.mysql

import delta.testing._
import delta.jdbc._
import delta.jdbc.mysql._

import scuff.EmailAddress

class TestMoreMySQLStreamProcessStore
extends TestMoreJdbcStreamProcessStore
with MySqlDataSource {

  import TestMoreStreamProcessStore._
  import TestMoreJdbcStreamProcessStore._

  override def newLookupStore() = {
    new ProcessStore(connSource)
    with MySQLSyntax
    with Lookup {
      protected def emailRef: String = EmailColumn.name
      protected def numRef: String = NumColumn.name
      protected def num2qry(num: Int) = num
      protected def email2qry(email: EmailAddress) = email
    }
  }.ensureTable()

  override def newDupeStore() = {
    new ProcessStore(connSource)
    with MySQLSyntax
    with DupeFinder {
      protected def emailRef: String = EmailColumn.name
      protected val getEmail: ReadColumn[EmailAddress] = SearchEmailColumn
    }
  }.ensureTable()

}
