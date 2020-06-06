package college.validation

import college.validation.EmailValidationProcess.State
import college.student._

import delta.validation._

import scala.concurrent.Future

import scuff.EmailAddress

class UniqueStudentEmailValidation(
  index: EmailIndex)
extends SetValidationBySeniority[Student.Id, State, Student] {

  type UniqueType = EmailAddress

  protected def compensate(duplicate: EmailAddress, student: Student): Unit =
    student apply RemoveStudentEmail(duplicate)

  protected def valuesFrom(state: State): List[EmailAddress] = state.asData.emails.toList

  protected def query(emailAddr: EmailAddress): Future[Map[Student.Id, Tick]] =
    index.lookupAll(emailAddr)

}
