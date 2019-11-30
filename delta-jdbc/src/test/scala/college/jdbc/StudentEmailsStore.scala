package college.jdbc

import scuff.jdbc.ConnectionSource
import delta.jdbc._
import college.TestCollege.StudentEmails
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scuff.JavaSerializer
import java.sql.ResultSet

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
  extends JdbcStreamProcessStore[Int, StudentEmails](
    "student_id", cs, version,
    table = "student_email_lookup", None,
    blockingCtx, withTimestamp)
  with IndexTables[Int, StudentEmails] {

  protected val indexTables = Table(EmailColumn)(_.emails) :: Nil

  private implicit def ec = blockingCtx

  def lookup(email: String): Future[Option[Int]] = {
    this.queryTick(EmailColumn -> email).map { allMatches =>
      assert(allMatches.size < 2)
      allMatches.headOption.map(_._1)
    }
  }

}
