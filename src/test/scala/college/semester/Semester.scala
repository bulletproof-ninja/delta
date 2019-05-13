package college.semester

import college._
import delta.ddd._
import delta.Projector
import college.student.Student

object Semester extends Entity("semester", SemesterAssembler) {
  type Id = IntId[Semester]
  type Type = Semester

  def apply(cmd: CreateClass): Semester = {
    val semester = new Semester
    semester(cmd)
    semester
  }

  def init(state: State, mergeEvents: List[SemesterEvent]): Semester = new Semester(state)

  def state(instance: Semester) = instance.state

  def validate(state: SemesterState) = require(state != null)

}

private[semester] object SemesterAssembler extends Projector[SemesterState, SemesterEvent] {
  def init(evt: SemesterEvent) = evt match {
    case ClassCreated(name) => new SemesterState(name)
    case _ => ???
  }
  def next(semester: SemesterState, evt: SemesterEvent) = evt match {
    case StudentEnrolled(studentId) =>
      val enrolled = semester.enrolled + studentId
      semester.copy(enrolled = enrolled)
    case StudentCancelled(studentId) =>
      val without = semester.enrolled - studentId
      semester.copy(enrolled = without)
    case _ => ???
  }
}

class Semester private (
    private[Semester] val state: Semester.State = Semester.newState()) {

  private def semester = state.curr

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
