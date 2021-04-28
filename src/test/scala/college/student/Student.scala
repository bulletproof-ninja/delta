package college.student

import college._
import delta.write._
import scuff.EmailAddress

object Student extends Entity("student", StudentState) {
  type Id = IntId[Student]
  type Type = Student

  def init(id: Id, state: StateRef, concurrentUpdates: List[Transaction]) = new Student(state)

  def StateRef(student: Student) = student.ref
  def validate(state: StudentState) = require(state != null)

  def apply(cmd: RegisterStudent): Student = {
    val student = new Student
    student(cmd)
    student
  }
}

class Student private[student] (val ref: Student.StateRef = Student.newStateRef()) {

  private def student = ref.get

  private[student] def apply(cmd: RegisterStudent): Unit = {
    require(student == null)
    ref apply StudentRegistered(cmd.name)
    ref apply StudentEmailAdded(cmd.email.toString)
  }

  def apply(cmd: ChangeStudentName): Unit = {
    val newName = cmd.newName.trim
    if (newName.length == 0) sys.error("No name supplied")
    if (newName != student.name) {
      ref apply StudentChangedName(newName)
    }
  }

  def apply(cmd: AddStudentEmail): Unit = {
    val addEmail = cmd.email.toLowerCase
    if (!student.emails.contains(addEmail)) {
      ref apply StudentEmailAdded(cmd.email.toString)
    }
  }

  def apply(cmd: RemoveStudentEmail): Unit = {
    val removeEmail = cmd.email.toLowerCase
    if (student.emails contains removeEmail) {
      ref apply StudentEmailRemoved(cmd.email.toString)
    }
  }

}

case class RegisterStudent(name: String, email: EmailAddress)
case class ChangeStudentName(newName: String)
case class AddStudentEmail(email: EmailAddress)
case class RemoveStudentEmail(email: EmailAddress)
