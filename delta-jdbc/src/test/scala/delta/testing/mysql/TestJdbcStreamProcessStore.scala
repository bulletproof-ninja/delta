package delta.testing.mysql

import scala.concurrent.duration.DurationInt

import org.junit._, Assert._

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource

import delta.Snapshot
import delta.jdbc.{ JdbcStreamProcessHistory, JdbcStreamProcessStore, VarCharColumn }
import delta.jdbc.mysql.MySQLSyntax
import delta.testing.{ RandomDelayExecutionContext, TestStreamProcessStore }
import delta.util.StreamProcessStore
import scuff.concurrent._
import scuff.jdbc.DataSourceConnection
import java.sql.Connection

object TestJdbcStreamProcessStore {

  implicit def ec = RandomDelayExecutionContext

  val db = "delta_testing"
  val readModelName = "readmodel_test"
  val ds = {
    val ds = new MysqlDataSource
    ds setUrl s"jdbc:mysql://localhost/$db"
    ds setCreateDatabaseIfNotExist true
    ds setUseUnicode true
    ds setRewriteBatchedStatements false
    ds setAutoReconnect true
    ds setCharacterEncoding "UTF-8"
    ds setDatabaseName db
    ds setUser "root"
    ds
  }

  @AfterClass
  def dropDb() {
    val conn = ds.getConnection
    try {
      val stm = conn.createStatement()
      try stm.execute(s"drop database $db") finally stm.close()
    } finally conn.close()
  }

  implicit object StringColumn extends VarCharColumn(255)
}

class TestJdbcStreamProcessStore
  extends TestStreamProcessStore {
  import TestJdbcStreamProcessStore._

  override def newStore: StreamProcessStore[Long, String] =
    new JdbcStreamProcessStore[Long, String](RandomDelayExecutionContext, 1, readModelName) with MySQLSyntax with DataSourceConnection {
      def dataSource = ds
      override def getConnection = {
        val conn = super.getConnection
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
        conn
      }
    }.ensureTable()

  @Test
  def mock() {}
}

class TestJdbcStreamProcessHistory
  extends TestJdbcStreamProcessStore {
  import TestJdbcStreamProcessStore._

  override def newStore: StreamProcessStore[Long, String] = newStore(1)
  private def newStore(v: Short) =
    new JdbcStreamProcessHistory[Long, String](RandomDelayExecutionContext, v, readModelName) with MySQLSyntax with DataSourceConnection {
      val dataSource = ds
    }.ensureTable()

  private def history(id: Long, store: StreamProcessStore[Long, String], dataPrefix: String): Unit = {
    store.write(id, Snapshot("{}", 0, 3L)).await(1.hour)
    store.refresh(id, 1, 5L).await(1.hour)
    assertEquals(Snapshot("{}", 1, 5L), store.read(id).await(1.hour).get)

    store.refresh(id, 2, 8).await(1.hour)
    assertEquals(Snapshot("{}", 2, 8L), store.read(id).await(1.hour).get)

    try {
      store.write(id, Snapshot(s"$dataPrefix{}", 1, 5)).await(1.hour)
      fail("Should fail on older tick")
    } catch {
      case _: IllegalStateException => // Expected
    }
    val snap_2_8 = Snapshot(s"$dataPrefix{}", 2, 8)
    store.write(id, snap_2_8).await(1.hour)
    assertEquals(Some(snap_2_8), store.read(id).await(1.hour))

    val snap_3_34 = Snapshot(s"""$dataPrefix{"hello":", world!"}""", 3, 34)
    store.write(id, snap_3_34).await(1.hour)
    assertEquals(Some(snap_3_34), store.read(id).await(1.hour))
  }

  @Test
  def history() {
    val id = util.Random.nextLong()
    val store = newStore(1)
    history(id, store, "")

    val store_v2 = newStore(2)
    history(id, store_v2, "JSON: ")
  }

}
