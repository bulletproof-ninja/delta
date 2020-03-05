package college.jdbc

import scuff.jdbc.ConnectionSource
import delta.jdbc._
import college.TestCollege._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scuff.JavaSerializer
import java.sql.ResultSet
import JdbcStreamProcessStore.Config

private object StudentEmailsStore {
  val EmailColumn = "email_address"
  implicit val EmailColumnType = VarCharColumn(255)
  implicit def ModelColumnType(implicit col: ColumnType[Array[Byte]]) =
    new ColumnType[StudentEmails] {
      val serializer = JavaSerializer[StudentEmails]
      def typeName = col.typeName
      override def writeAs(value: StudentEmails) = col.writeAs(serializer encode value)
      def readFrom(rs: ResultSet, colIdx: Int) = serializer decode col.readFrom(rs, colIdx)
    }
}

import StudentEmailsStore._

class StudentEmailsStore(
    cs: ConnectionSource, version: Short, withTimestamp: WithTimestamp,
    blockingCtx: ExecutionContext)(
        implicit colType: ColumnType[Array[Byte]])
  extends JdbcStreamProcessStore[Int, StudentEmails, StudentEmailsUpdate](
    Config("student_id", table = "student_email_lookup").version(version) timestamp withTimestamp,
    cs, blockingCtx)
  with IndexTables[Int, StudentEmails, StudentEmailsUpdate] {

  protected val indexTables = Table(EmailColumn)(_.emails) :: Nil

  private implicit def ec = blockingCtx

  def lookup(email: String): Future[Option[Int]] = {
    this.queryTick(EmailColumn -> email).map { allMatches =>
      assert(allMatches.size < 2)
      allMatches.headOption.map(_._1)
    }
  }

}
