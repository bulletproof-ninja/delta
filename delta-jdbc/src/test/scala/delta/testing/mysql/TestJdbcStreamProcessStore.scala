package delta.testing.mysql

import scala.concurrent.duration.DurationInt

import org.junit._, Assert._

//import com.mysql.jdbc.log.Slf4JLogger
//import com.mysql.jdbc.jdbc2.optional.MysqlDataSource
import com.mysql.cj.log.Slf4JLogger
import com.mysql.cj.jdbc.MysqlDataSource

import delta.Snapshot
import delta.jdbc.{ JdbcStreamProcessHistory, JdbcStreamProcessStore, VarCharColumn }
import delta.jdbc.mysql.MySQLSyntax
import delta.testing.{ RandomDelayExecutionContext, TestStreamProcessStore }
import delta.process.StreamProcessStore
import scuff.concurrent._
import scuff.jdbc.{ ConnectionSource, DataSourceConnection }
import java.sql.Connection
import delta.jdbc.ColumnType
import delta.testing.Foo
import scala.util.Random
import scala.concurrent.Future

object TestJdbcStreamProcessStore {

  implicit def ec = RandomDelayExecutionContext

  val db = "delta_testing"
  val readModelName = "readmodel_test"
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

  implicit object StringColumn extends VarCharColumn(255)
}

class TestJdbcStreamProcessStore
  extends TestStreamProcessStore {
  import TestJdbcStreamProcessStore._

  private val cs = new ConnectionSource with DataSourceConnection {
    def dataSource = ds
    override def getConnection = {
      val conn = super.getConnection
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
      conn
    }
  }

  override def newStore(): StreamProcessStore[Long, String] = {
    val store = new JdbcStreamProcessStore[Long, String](
      cs, None, readModelName, None, RandomDelayExecutionContext) with MySQLSyntax
    store.ensureTable()
  }

  private val fooTable = s"foo_${Random.nextInt.toHexString}"

  def fooVersion: Short = 1

  object FooProcessStore {
    import JdbcStreamProcessStore._
    val qryColumns = List(
      Index(Nullable("foo_text")((foo: Foo) => Option(foo.text))),
      Index(NotNull("foo_num")((foo: Foo) => foo.num)))
    implicit val FooColumn = ColumnType(Foo)
  }
  class FooProcessStore(cs: ConnectionSource, version: Short)(implicit fooCol: ColumnType[Foo])
    extends JdbcStreamProcessStore[Long, Foo](
      FooProcessStore.qryColumns, cs, Some(version), fooTable, None, RandomDelayExecutionContext) {
    def queryText(text: String): Future[Map[Long, Snapshot]] = {
      this.querySnapshot("foo_text" -> text)
    }
  }
  override def newFooStore = {
    import FooProcessStore._
    val store = new FooProcessStore(cs, fooVersion) with MySQLSyntax
    store.ensureTable()
  }

  override def foo(): Unit = {
    super.foo()
    val fooStore = newFooStore
    fooStore.write(567567, Snapshot(Foo("Foo", 9999), 99, 999999L))
    val result = fooStore.queryText("Foo").await
    assertTrue(result.nonEmpty)
    result.values.foreach {
      case Snapshot(foo, _, _) => assertEquals("Foo", foo.text)
    }
  }

  @Test
  def mock() = ()
}

class TestJdbcStreamProcessHistory
  extends TestJdbcStreamProcessStore {
  import TestJdbcStreamProcessStore._

  override def newStore(): StreamProcessStore[Long, String] = newStore(1)
  private def newStore(v: Int) = {
    val cs = new ConnectionSource with DataSourceConnection {
      def dataSource = ds
    }
    val store = new JdbcStreamProcessHistory[Long, String](RandomDelayExecutionContext, cs, v.toShort, readModelName) with MySQLSyntax
    store.ensureTable()
  }

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
  def history(): Unit = {
    val id = util.Random.nextLong()
    val store = newStore(1)
    history(id, store, "")

    val store_v2 = newStore(2)
    history(id, store_v2, "JSON: ")
  }

}
