package college.student

import delta.ddd._
import college.StudentId
import delta.Fold

object Student extends Entity {

  type Id = StudentId
  type Type = Student
  type Mutator = Student

  def newMutator = new Student

  def init(student: Student, mergeEvents: List[StudentEvent]) = student

  def done(student: Student) = student

  def apply(cmd: RegisterStudent): Student = {
    val student = new Student
    student(cmd)
    student
  }
}

class Student private () extends StateMutator {

  type Event = StudentEvent
  type State = college.student.State

  protected val fold = new Fold[State, StudentEvent] {
    def init(evt: StudentEvent) = evt match {
      case StudentRegistered(name) => new State(name)
    }
    def next(student: State, evt: StudentEvent) = evt match {
      case StudentChangedName(newName) => student.copy(name = newName)
    }

  }

  private def student = state

  private def apply(cmd: RegisterStudent) {
    require(student == null)
    apply(StudentRegistered(cmd.name))
  }
  def apply(cmd: ChangeStudentName) {
    val newName = cmd.newName.trim
    if (newName.length == 0) sys.error("No name supplied")
    if (newName != student.name) {
      apply(StudentChangedName(newName))
    }
  }

}

case class RegisterStudent(name: String)
case class ChangeStudentName(newName: String)
