package college.semester

import college._
import ulysses.ddd._

object Semester extends Entity {
  def apply(cmd: CreateClass): Semester = {
    val semester = new Semester
    semester(cmd)
    semester
  }

  type Id = SemesterId
  type Type = Semester
  type Event = SemesterEvent
  type State = college.semester.State

  def newMutator(state: Option[State]): StateMutator[Event, State] = new SemesterMutator(state.orNull)

  def init(state: State, mergeEvents: List[Event]): Type = new Semester(state)

  /**
    * Get the mutator used for the entity instance.
    * @param entity The instance to get mutator from
    */
  def done(instance: Type): StateMutator[Event, State] = instance.mutator

  def checkInvariants(state: State): Unit = ()

}

private class SemesterMutator(private var semester: State = null)
    extends StateMutator[SemesterEvent, State] {

  protected def process(evt: SemesterEvent) {
    evt match {
      case ClassCreated(name) =>
        semester = new State(name)
      case StudentEnrolled(studentId) =>
        val enrolled = semester.enrolled + studentId
        semester = semester.copy(enrolled = enrolled)
      case StudentCancelled(studentId) =>
        val without = semester.enrolled - studentId
        semester = semester.copy(enrolled = without)
    }
  }
  def state = semester
}

class Semester private (
    private[Semester] val mutator: SemesterMutator = new SemesterMutator) {

  private def this(state: State) = this(new SemesterMutator(state))

  private def semester = mutator.state

  def apply(cmd: CreateClass) {
    require(semester == null)
    mutator(ClassCreated(cmd.className))
  }

  def apply(cmd: EnrollStudent) {
    if (!semester.enrolled(cmd.student)) {
      mutator(StudentEnrolled(cmd.student))
    }
  }
  def apply(cmd: CancelStudent) {
    if (semester.enrolled(cmd.student)) {
      mutator(StudentCancelled(cmd.student))
    }
  }
}

case class CreateClass(className: String)
case class EnrollStudent(student: StudentId)
case class CancelStudent(student: StudentId)
