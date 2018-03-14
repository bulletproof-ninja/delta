package college.student

import college._
import delta.ddd._
import delta.EventReducer

object Student extends Entity[Student, StudentState, StudentEvent](StudentAssembler) {
  type Id = StudentId

  def init(state: State, mergeEvents: List[StudentEvent]) = new Student(state)

  def state(student: Student) = student.state
  def validate(state: StudentState) = require(state != null)

  def apply(cmd: RegisterStudent): Student = {
    val student = new Student
    student(cmd)
    student
  }
}

object StudentAssembler extends EventReducer[StudentState, StudentEvent] {
  def init(evt: StudentEvent) = evt match {
    case StudentRegistered(name) => new StudentState(name)
  }
  def next(student: StudentState, evt: StudentEvent) = evt match {
    case StudentChangedName(newName) => student.copy(name = newName)
  }

}

class Student private[student] (val state: Student.State = Student.newState()) {

  private def student = state.curr

  private[student] def apply(cmd: RegisterStudent) {
    require(student == null)
    state(StudentRegistered(cmd.name))
  }
  def apply(cmd: ChangeStudentName) {
    val newName = cmd.newName.trim
    if (newName.length == 0) sys.error("No name supplied")
    if (newName != student.name) {
      state(StudentChangedName(newName))
    }
  }

}

case class RegisterStudent(name: String)
case class ChangeStudentName(newName: String)
