package college.student

import ulysses.ddd.StateMutator
import ulysses.ddd.AggregateRoot
import college.StudentId

object Student extends AggregateRoot {

  type Id = StudentId
  type Channel = String
  type Entity = Student
  type Event = StudentEvent
  type State = college.student.State

  val channel: Channel = "Student"

  def newMutator(state: Option[State]) = new Student(state.orNull)

  def init(state: State, mergeEvents: List[Event]) = new Student(state)

  def done(student: Student) = student

  def checkInvariants(state: State): Unit = ()

  def apply(cmd: RegisterStudent): Student = {
    val student = new Student
    student(cmd)
    student
  }
}

class Student private (
  private var student: State = null)
    extends StateMutator[StudentEvent, State] {

  def state = student
  protected def process(evt: StudentEvent) {
    evt match {
      case StudentRegistered(name) =>
        student = new State(name)
      case StudentChangedName(newName) =>
        student = student.copy(name = newName)
    }
  }

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
