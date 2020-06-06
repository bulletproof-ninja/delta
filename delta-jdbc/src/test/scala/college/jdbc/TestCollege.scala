package college.jdbc

import scuff.jdbc.AsyncConnectionSource
import college.validation.EmailValidationProcessStore

abstract class TestCollege extends college.TestCollege {

  def connSource: AsyncConnectionSource
  override def newEmailValidationProcessStore(): EmailValidationProcessStore
  override def newEventStore(): college.CollegeEventStore

}
