package college.jdbc

import scuff.jdbc.AsyncConnectionSource
import college.validation.EmailValidationProcessStore
import delta.Channel
import delta.MessageTransport

abstract class TestCollege extends college.TestCollege {

  def connSource: AsyncConnectionSource
  override def newEmailValidationProcessStore(): EmailValidationProcessStore
  override def newEventStore[MT](
      allChannels: Set[Channel],
      txTransport: MessageTransport[MT])(
      implicit
      encode: Transaction => MT,
      decode: MT => Transaction)
      : college.CollegeEventStore

}
