package college.validation

import college.validation.EmailValidationProcess.State
import college.student._

import delta.validation._

import scala.concurrent.Future

import scuff.EmailAddress

class UniqueStudentEmailValidation(
  index: EmailIndex)
extends SetValidationBySeniority[Student.Id, State, Student] {

  type Qualifier = EmailAddress

  protected def needValidation(state: State): Set[EmailAddress] =
    state.asData.newEmails

  protected def findMatches(emailAddr: EmailAddress): Future[Map[Student.Id, Tick]] =
    index.lookupAll(emailAddr)

  protected def compensate(duplicate: EmailAddress, validatedState: State, student: Student): Unit =
    student apply RemoveStudentEmail(duplicate)

}
