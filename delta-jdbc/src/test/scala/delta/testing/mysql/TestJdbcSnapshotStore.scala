package delta.testing.mysql

import delta.testing.TestSnapshotStore
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource
import org.junit._
import delta.SnapshotStore
import delta.jdbc.JdbcSnapshotStore
import scuff.jdbc.DataSourceConnection
import delta.testing.RandomDelayExecutionContext
import delta.jdbc.VarCharColumn
import delta.jdbc.mysql.MySQLSnapshotStore

object TestJdbcSnapshotStore {

  implicit def ec = RandomDelayExecutionContext

  val db = "delta_testing"
  val ds = {
    val ds = new MysqlDataSource
    ds.setUser("root")
    ds setUrl s"jdbc:mysql://localhost/$db?createDatabaseIfNotExist=true&useUnicode=true&characterEncoding=utf-8&autoReconnect=true"
    ds
  }

  @AfterClass
  def dropDb {
    val conn = ds.getConnection
    try {
      val stm = conn.createStatement()
      try stm.execute(s"drop database $db") finally stm.close()
    } finally conn.close()
  }

  implicit object StringColumn extends VarCharColumn(255)
}

class TestJdbcSnapshotStore
    extends TestSnapshotStore {
  import TestJdbcSnapshotStore._

  override val store: SnapshotStore[Long, String] =
    new JdbcSnapshotStore[Long, String](
      "readmodel_test") with DataSourceConnection with MySQLSnapshotStore {
      val dataSource = ds
    }.ensureTable(dropIfExists = true)

  @Test
  def mock() {}
}
