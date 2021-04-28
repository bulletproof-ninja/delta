package delta.testing.mysql

import delta.testing.BaseTest
import com.mysql.cj.jdbc.MysqlDataSource
import scuff.SysProps
import scala.concurrent.ExecutionContext
import scuff.jdbc.AsyncConnectionSource
import java.sql.Connection

trait MySqlDataSource
extends BaseTest {
  private lazy val debug = SysProps.optional("delta.mysql.debug").map(_ != "false") getOrElse false
  System.setProperty("delta.mysql.password", "mxccvlwer$562342kjasdfsaf345xcvb")
  val db = s"delta_testing_$instance"
  lazy val mysqlDataSource = {
    val ds = new MysqlDataSource
    ds setUser "root"
    ds setPassword SysProps.required("delta.mysql.password")
    ds setURL s"jdbc:mysql://localhost/$db"
    ds setCreateDatabaseIfNotExist true
    ds setAutoReconnect true
    ds setCharacterEncoding "utf-8"
    ds setRewriteBatchedStatements false
    ds setContinueBatchOnError false
    ds setProfileSQL debug
    if (debug) {
      ds setLogger classOf[com.mysql.cj.log.StandardLogger].getName
      System setErr printStream
    }
    ds
  }
  abstract override def afterAll() = {
    if (debug) printStream.close()
    super.afterAll()
  }
  lazy val connSource = new AsyncConnectionSource {
    def updateContext: ExecutionContext = ec
    def queryContext: ExecutionContext = ec
    protected def getConnection: Connection = {
      val conn = mysqlDataSource.getConnection()
      conn setTransactionIsolation Connection.TRANSACTION_READ_COMMITTED
      conn
    }
  }

}
