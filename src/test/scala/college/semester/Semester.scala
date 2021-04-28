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

  def init(id: Id, stateRef: StateRef, concurrentUpdates: List[Transaction]) =
    new Semester(stateRef)

  def StateRef(instance: Semester) = instance.stateRef

  def validate(state: SemesterState) = require(state != null)

}

class Semester private (
    private[Semester] val stateRef: Semester.StateRef = Semester.newStateRef()) {

  private def semester = stateRef.get

  private def apply(cmd: CreateClass): Unit = {
    require(semester == null)
    stateRef apply ClassCreated(cmd.className)
  }

  def apply(cmd: EnrollStudent): Unit = {
    if (!semester.enrolled(cmd.student)) {
      stateRef apply StudentEnrolled(cmd.student)
    }
  }
  def apply(cmd: CancelStudent): Unit = {
    if (semester.enrolled(cmd.student)) {
      stateRef apply StudentCancelled(cmd.student)
    }
  }
}

case class CreateClass(className: String)
case class EnrollStudent(student: Student.Id)
case class CancelStudent(student: Student.Id)
