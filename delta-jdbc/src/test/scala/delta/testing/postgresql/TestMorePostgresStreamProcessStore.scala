package delta.testing.postgresql

import delta.testing._

import scala.concurrent._

import org.junit._

import delta.jdbc._

import scuff.SysProps
import scuff.jdbc._

import scuff.EmailAddress

import org.postgresql.ds.PGSimpleDataSource

object TestMorePostgresStreamProcessStore {


  implicit def ec = RandomDelayExecutionContext

  val schema = s"delta_testing_$randomName"

  val cs = new AsyncConnectionSource with DataSourceConnection {

    override def updateContext: ExecutionContext = RandomDelayExecutionContext

    override def queryContext: ExecutionContext = RandomDelayExecutionContext

    val dataSource = {
      val ds = new PGSimpleDataSource
      ds.setUser("postgres")
      ds.setPassword(SysProps.required("delta.postgresql.password"))
      ds setUrl s"jdbc:postgresql://localhost/"
      ds setCurrentSchema schema
      ds
    }
  }

  @AfterClass
  def cleanup(): Unit = {
    cs.asyncUpdate {
      _.createStatement().execute(s"DROP SCHEMA $schema CASCADE")
    }.await
  }
}

class TestMorePostgresStreamProcessStore
extends TestMoreJdbcStreamProcessStore {

  import TestMoreStreamProcessStore._
  import TestMoreJdbcStreamProcessStore._
  import TestMorePostgresStreamProcessStore._

  override def newLookupStore() = {
    new ProcessStore(cs, schema)
    with Lookup {
      protected def emailRef: String = EmailColumn.name
      protected def numRef: String = NumColumn.name
      protected def num2qry(num: Int) = num
      protected def email2qry(email: EmailAddress) = email
    }
  }.ensureTable()

  override def newDupeStore() = {
    new ProcessStore(cs, schema)
    with DupeFinder {
      protected def emailRef: String = EmailColumn.name
      protected val getEmail: ReadColumn[EmailAddress] = SearchEmailColumn
    }
  }.ensureTable()

  @Test
  def dummy() = ()
}
