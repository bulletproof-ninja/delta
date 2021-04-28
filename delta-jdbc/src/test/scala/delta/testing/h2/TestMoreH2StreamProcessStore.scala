package delta.testing.h2

import delta.testing._

import scala.concurrent._

import delta.jdbc._

import scuff.jdbc._

import scuff.EmailAddress
import org.h2.jdbcx.JdbcDataSource
import java.io.File
import scala.util.Random

class TestMoreH2StreamProcessStore
extends TestMoreJdbcStreamProcessStore {

  val h2Name = s"delete-me.h2db.${Random.nextInt().abs}"
  val h2File = new File(".", h2Name + ".mv.db")

  val cs = new AsyncConnectionSource with DataSourceConnection {

    override def updateContext: ExecutionContext = ec

    override def queryContext: ExecutionContext = ec

    val dataSource = new JdbcDataSource
    dataSource.setURL(s"jdbc:h2:./${h2Name}")
  }


  override def afterAll(): Unit = {
    h2File.delete()
    super.afterAll()
  }

  import TestMoreStreamProcessStore._
  import TestMoreJdbcStreamProcessStore._

  override def newLookupStore() = {
    new ProcessStore(cs) with Lookup {
      protected def emailRef: String = EmailColumn.name
      protected def numRef: String = NumColumn.name
      protected def num2qry(num: Int) = num
      protected def email2qry(email: EmailAddress) = email
    }
  }.ensureTable()

  override def newDupeStore() = {
    new ProcessStore(cs) with DupeFinder {
      protected def emailRef: String = EmailColumn.name
      protected val getEmail: ReadColumn[EmailAddress] = SearchEmailColumn
    }
  }.ensureTable()

}
