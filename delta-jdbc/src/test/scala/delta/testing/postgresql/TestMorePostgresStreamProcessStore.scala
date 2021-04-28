package delta.testing.postgresql

import delta.testing._

import scala.concurrent._

import delta.jdbc._

import scuff.SysProps
import scuff.jdbc._

import scuff.EmailAddress

import org.postgresql.ds.PGSimpleDataSource

class TestMorePostgresStreamProcessStore
extends TestMoreJdbcStreamProcessStore {

  val schema = s"delta_testing_$randomName"

  val cs = new AsyncConnectionSource with DataSourceConnection {

    override def updateContext: ExecutionContext = ec

    override def queryContext: ExecutionContext = ec

    val dataSource = {
      val ds = new PGSimpleDataSource
      ds.setUser("postgres")
      ds.setPassword(SysProps.required("delta.postgresql.password"))
      ds setUrl s"jdbc:postgresql://localhost/"
      ds setCurrentSchema schema
      ds
    }
  }

  override def afterAll(): Unit = {
    cs.asyncUpdate {
      _.createStatement().execute(s"DROP SCHEMA $schema CASCADE")
    }.await
    super.afterAll()
  }

  import TestMoreStreamProcessStore._
  import TestMoreJdbcStreamProcessStore._

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

}
