package college.semester

import college._
import delta.ddd._
import delta.EventReducer

object Semester extends Entity[Semester, SemesterState, SemesterEvent](SemesterAssembler) {
  type Id = SemesterId

  def apply(cmd: CreateClass): Semester = {
    val semester = new Semester
    semester(cmd)
    semester
  }

  def init(state: State, mergeEvents: List[SemesterEvent]): Semester = new Semester(state)

  def state(instance: Semester) = instance.state

  def validate(state: SemesterState) = require(state != null)

}

private[semester] object SemesterAssembler extends EventReducer[SemesterState, SemesterEvent] {
  def init(evt: SemesterEvent) = evt match {
    case ClassCreated(name) => new SemesterState(name)
  }
  def next(semester: SemesterState, evt: SemesterEvent) = evt match {
    case StudentEnrolled(studentId) =>
      val enrolled = semester.enrolled + studentId
      semester.copy(enrolled = enrolled)
    case StudentCancelled(studentId) =>
      val without = semester.enrolled - studentId
      semester.copy(enrolled = without)
  }
}

class Semester private (
    private[Semester] val state: Semester.State = Semester.newState()) {

  private def semester = state.curr

  private def apply(cmd: CreateClass) {
    require(semester == null)
    state(ClassCreated(cmd.className))
  }

  def apply(cmd: EnrollStudent) {
    if (!semester.enrolled(cmd.student)) {
      state(StudentEnrolled(cmd.student))
    }
  }
  def apply(cmd: CancelStudent) {
    if (semester.enrolled(cmd.student)) {
      state(StudentCancelled(cmd.student))
    }
  }
}

case class CreateClass(className: String)
case class EnrollStudent(student: StudentId)
case class CancelStudent(student: StudentId)
