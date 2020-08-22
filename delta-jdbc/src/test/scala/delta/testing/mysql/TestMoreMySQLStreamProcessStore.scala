package delta.testing.mysql

import delta.testing._

import scala.concurrent._

import org.junit._

import com.mysql.cj.log.Slf4JLogger
import com.mysql.cj.jdbc.MysqlDataSource

import delta.jdbc._

import delta.jdbc.mysql._

import scuff.SysProps
import scuff.jdbc._

import java.sql._
import scuff.EmailAddress

object TestMoreMySQLStreamProcessStore {

  implicit def ec = RandomDelayExecutionContext

  val db = s"delta_testing_$randomName"

  val ds = {
    val ds = new MysqlDataSource
    ds setServerName "localhost"
    ds setDatabaseName db
    ds setCreateDatabaseIfNotExist true
    ds setRewriteBatchedStatements false
    ds setContinueBatchOnError false
    ds setProfileSQL true
    ds setAutoReconnect true
    ds setCharacterEncoding "UTF-8"
    ds setDatabaseName db
    ds setUser "root"
    ds setPassword SysProps.required("delta.mysql.password")
    ds setUseSSL false
//    ds setLoggerClassName classOf[Slf4JLogger].getName
    ds setLogger classOf[Slf4JLogger].getName
    ds
  }

  @AfterClass
  def dropDb(): Unit = {
    val conn = ds.getConnection
    try {
      val stm = conn.createStatement()
      try stm.execute(s"drop database $db") finally stm.close()
    } finally conn.close()
  }

}

class TestMoreMySQLStreamProcessStore
extends TestMoreJdbcStreamProcessStore {

  import TestMoreStreamProcessStore._
  import TestMoreJdbcStreamProcessStore._
  import TestMoreMySQLStreamProcessStore._

  private val cs = new AsyncConnectionSource with DataSourceConnection {

    override def updateContext: ExecutionContext = RandomDelayExecutionContext

    override def queryContext: ExecutionContext = RandomDelayExecutionContext

    def dataSource = ds
    override def getConnection = {
      val conn = super.getConnection
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
      conn
    }
  }

  override def newLookupStore() = {
    new ProcessStore(cs)
    with MySQLSyntax
    with Lookup {
      protected def emailRef: String = EmailColumn.name
      protected def numRef: String = NumColumn.name
      protected def num2qry(num: Int) = num
      protected def email2qry(email: EmailAddress) = email
    }
  }.ensureTable()

  override def newDupeStore() = {
    new ProcessStore(cs)
    with MySQLSyntax
    with DupeFinder {
      protected def emailRef: String = EmailColumn.name
      protected val getEmail: ReadColumn[EmailAddress] = SearchEmailColumn
    }
  }.ensureTable()

  @Test
  def dummy() = ()
}
