package college.student

import scuff.EmailAddress
import scuff.concurrent.Threads.PiggyBack

import scala.concurrent.Future

import delta.process.StreamProcessStore
import delta.validation._
import college.validation.EmailValidationProcess.State

trait EmailIndex {
  processStore: StreamProcessStore[Int, State, Unit] =>

  def lookupAll(email: EmailAddress): Future[Map[Student.Id, Tick]]
  def lookup(email: EmailAddress): Future[Option[Student.Id]] =
    lookupAll(email).map(pickOldest)(PiggyBack)

}
