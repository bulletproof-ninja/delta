package college.semester

import college.student.Student
import delta.Projector

case class SemesterState(
  name: String,
  enrolled: Set[Student.Id] = Set.empty)

object SemesterState extends Projector[SemesterState, SemesterEvent] {
  def init(evt: SemesterEvent) = next(null, evt)
  def next(semester: SemesterState, evt: SemesterEvent) = evt match {
    case ClassCreated(name) => assert(semester == null); new SemesterState(name)
    case StudentEnrolled(studentId) =>
      val enrolled = semester.enrolled + studentId
      semester.copy(enrolled = enrolled)
    case StudentCancelled(studentId) =>
      val without = semester.enrolled - studentId
      semester.copy(enrolled = without)
  }
}
