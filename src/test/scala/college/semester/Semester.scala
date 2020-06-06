package college.semester

import college._
import delta.write._
import college.student.Student

object Semester extends Entity("semester", SemesterState) {
  type Id = IntId[Semester]
  type Type = Semester

  def apply(cmd: CreateClass): Semester = {
    val semester = new Semester
    semester(cmd)
    semester
  }

  def init(state: State, concurrentUpdates: List[Transaction]) =
    new Semester(state)

  def state(instance: Semester) = instance.state

  def validate(state: SemesterState) = require(state != null)

}

class Semester private (
    private[Semester] val state: Semester.State = Semester.newState()) {

  private def semester = state.get

  private def apply(cmd: CreateClass): Unit = {
    require(semester == null)
    state(ClassCreated(cmd.className))
  }

  def apply(cmd: EnrollStudent): Unit = {
    if (!semester.enrolled(cmd.student)) {
      state(StudentEnrolled(cmd.student))
    }
  }
  def apply(cmd: CancelStudent): Unit = {
    if (semester.enrolled(cmd.student)) {
      state(StudentCancelled(cmd.student))
    }
  }
}

case class CreateClass(className: String)
case class EnrollStudent(student: Student.Id)
case class CancelStudent(student: Student.Id)
