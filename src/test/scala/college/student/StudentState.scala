package college.student

import delta.Projector

case class StudentState(
  name: String,
  emails: Set[String] = Set.empty)

object StudentState extends Projector[StudentState, StudentEvent] {
  def init(evt: StudentEvent) = next(null, evt)
  def next(student: StudentState, evt: StudentEvent) = evt match {
    case StudentRegistered(name) => assert(student == null); new StudentState(name)
    case StudentChangedName(newName) => student.copy(name = newName)
    case StudentEmailAdded(newEmail) => student.copy(emails = student.emails + newEmail.toLowerCase)
    case StudentEmailRemoved(removeEmail) => student.copy(emails = student.emails - removeEmail.toLowerCase)
  }

}
