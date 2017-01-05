package college.jdbc.h2

import java.io.File

import org.h2.jdbcx.JdbcDataSource
import org.junit.AfterClass
import org.junit.Assert.assertTrue
import org.junit.Test

import college.CollegeEvent
import ulysses.EventStore
import ulysses.jdbc._
import ulysses.jdbc.h2.H2Dialect
import ulysses.util.LocalPublishing
import ulysses.testing.RandomDelayExecutionContext
import scala.util.Random
import java.sql.Connection

object TestCollege {
  val h2Name = s"delete-me.h2db.${Random.nextInt().abs}"
  val h2File = new File(".", h2Name + ".mv.db")
  @AfterClass
  def cleanup {
    h2File.delete()
  }
}

class TestCollege extends college.TestCollege {

  override lazy val eventStore: EventStore[Int, CollegeEvent, String] = {
    val sql = new H2Dialect[Int, CollegeEvent, String, Array[Byte]](None)
    val ds = new JdbcDataSource
    ds.setURL(s"jdbc:h2:./${TestCollege.h2Name}")
    new JdbcEventStore[Int, CollegeEvent, String, Array[Byte]](
      sql, RandomDelayExecutionContext) with LocalPublishing[Int, CollegeEvent, String] {
      def publishCtx = RandomDelayExecutionContext
      protected def useConnection[R](thunk: Connection => R): R = {
        val conn = ds.getConnection
        try thunk(conn)finally conn.close()
      }

    }
  }

  @Test
  def mock {
    assertTrue(true)
  }

}
