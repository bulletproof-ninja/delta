package delta.testing.postgresql

import org.postgresql.ds.PGSimpleDataSource
import scuff.SysProps
import org.postgresql.jdbc.AutoSave
import delta.testing.BaseTest

trait PostgresDataSource
extends BaseTest {

  val schema = s"delta_testing_$instance"

  lazy val postgresDataSource = {
    val ds = new PGSimpleDataSource
    // ds setLoggerLevel "DEBUG"
    // ds setLoggerFile logFile.toString
    ds setUser "postgres"
    ds.setPassword(SysProps required "delta.postgresql.password")
    ds setUrl s"jdbc:postgresql://localhost/"
    ds setAutosave AutoSave.NEVER
    ds
  }

}
