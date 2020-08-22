package delta.testing.h2

import delta.testing._

import scala.concurrent._

import org.junit._

import delta.jdbc._

import scuff.jdbc._

import scuff.EmailAddress
import org.h2.jdbcx.JdbcDataSource
import java.io.File
import scala.util.Random

object TestMoreH2StreamProcessStore {


  implicit def ec = RandomDelayExecutionContext

  val h2Name = s"delete-me.h2db.${Random.nextInt().abs}"
  val h2File = new File(".", h2Name + ".mv.db")

  val cs = new AsyncConnectionSource with DataSourceConnection {

    override def updateContext: ExecutionContext = RandomDelayExecutionContext

    override def queryContext: ExecutionContext = RandomDelayExecutionContext

    val dataSource = new JdbcDataSource
    dataSource.setURL(s"jdbc:h2:./${h2Name}")
  }


  @AfterClass
  def cleanup(): Unit = {
    h2File.delete()
  }

}

class TestMoreH2StreamProcessStore
extends TestMoreJdbcStreamProcessStore {

  import TestMoreStreamProcessStore._
  import TestMoreJdbcStreamProcessStore._
  import TestMoreH2StreamProcessStore._

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

  @Test
  def dummy() = ()
}
