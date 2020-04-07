package college.student

import college._
import delta.write._
import delta.Projector
import scuff.EmailAddress

object Student extends Entity("student", StudentProjector) {
  type Id = IntId[Student]
  type Type = Student

  def init(state: State, mergeEvents: List[StudentEvent]) = new Student(state)

  def state(student: Student) = student.state
  def validate(state: StudentState) = require(state != null)

  def apply(cmd: RegisterStudent): Student = {
    val student = new Student
    student(cmd)
    student
  }
}

object StudentProjector extends Projector[StudentState, StudentEvent] {
  def init(evt: StudentEvent) = next(null, evt)
  def next(student: StudentState, evt: StudentEvent) = evt match {
    case StudentRegistered(name, email) => assert(student == null); new StudentState(name, Set(email.toLowerCase))
    case StudentChangedName(newName) => student.copy(name = newName)
    case StudentEmailAdded(newEmail) => student.copy(emails = student.emails + newEmail.toLowerCase)
    case StudentEmailRemoved(removeEmail) => student.copy(emails = student.emails - removeEmail.toLowerCase)
  }

}

class Student private[student] (val state: Student.State = Student.newState()) {

  private def student = state.curr

  private[student] def apply(cmd: RegisterStudent): Unit = {
    require(student == null)
    state(StudentRegistered(cmd.name, cmd.email.toString))
  }

  def apply(cmd: ChangeStudentName): Unit = {
    val newName = cmd.newName.trim
    if (newName.length == 0) sys.error("No name supplied")
    if (newName != student.name) {
      state(StudentChangedName(newName))
    }
  }

  def apply(cmd: AddStudentEmail): Unit = {
    val addEmail = cmd.email.toLowerCase
    if (!student.emails.contains(addEmail)) {
      state(StudentEmailAdded(cmd.email.toString))
    }
  }

  def apply(cmd: RemoveStudentEmail): Unit = {
    val removeEmail = cmd.email.toLowerCase
    if (student.emails contains removeEmail) {
      state(StudentEmailRemoved(cmd.email.toString))
    }
  }

}

case class RegisterStudent(name: String, email: EmailAddress)
case class ChangeStudentName(newName: String)
case class AddStudentEmail(email: EmailAddress)
case class RemoveStudentEmail(email: EmailAddress)
