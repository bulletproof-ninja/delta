package college.jdbc

import scuff.jdbc.ConnectionSource
import delta.process.StreamProcessStore
import college.TestCollege.StudentEmails

abstract class TestCollege extends college.TestCollege {

  def connSource: ConnectionSource

  override def newLookupServiceProcStore: StreamProcessStore[Int, StudentEmails]

  override def lookupService(procStore: StreamProcessStore[Int, StudentEmails]) = procStore match {
    case store: StudentEmailsStore => new JdbcLookupService(store)
    case _ => sys.error(s"This method was called with unexpected implementation: ${procStore.getClass}")
  }

}
