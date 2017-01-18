package delta.testing.mysql

import delta.testing.TestReadModelStore
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource
import org.junit._
import delta.cqrs.ReadModelStore
import delta.jdbc.JdbcReadModelStore3
import delta.jdbc.DataSourceConnectionProvider
import delta.testing.RandomDelayExecutionContext
import delta.jdbc.VarCharColumn
import delta.jdbc.mysql.MySQLReadModelStore

object TestJdbcReadModelStore {

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

class TestJdbcReadModelStore
    extends TestReadModelStore {
  import TestJdbcReadModelStore._

  val store: ReadModelStore[(String, Long, Int), String] =
    new JdbcReadModelStore3[String, Long, Int, String]("readmodel_test"
        ) with DataSourceConnectionProvider with MySQLReadModelStore {
      val dataSource = ds
    }

  @Before def setup {
    store match {
      case store: JdbcReadModelStore3[_, _, _, _] => store.ensureTable()
    }
  }

  @Test
  def mock() {}
}
