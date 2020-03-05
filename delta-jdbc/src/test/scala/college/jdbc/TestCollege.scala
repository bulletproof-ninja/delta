package college.jdbc

import scuff.jdbc.ConnectionSource
import delta.process.StreamProcessStore
import college.TestCollege._

abstract class TestCollege extends college.TestCollege {

  def connSource: ConnectionSource

  override def newLookupServiceProcStore: StreamProcessStore[Int, StudentEmails, StudentEmailsUpdate]

  override def lookupService(procStore: StreamProcessStore[Int, StudentEmails, StudentEmailsUpdate]) = procStore match {
    case store: StudentEmailsStore => new JdbcLookupService(store)
    case _ => sys.error(s"This method was called with unexpected implementation: ${procStore.getClass}")
  }

}
