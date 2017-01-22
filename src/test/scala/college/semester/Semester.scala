package college.semester

import college._
import delta.ddd._
import delta.Fold

object Semester extends Entity {
  def apply(cmd: CreateClass): Semester = {
    val semester = new Semester
    semester(cmd)
    semester
  }

  type Id = SemesterId
  type Type = Semester
  type Mutator = SemesterMutator
//  type Event = SemesterEvent
//  type State = college.semester.State

  def newMutator = new SemesterMutator

  def init(mutator: SemesterMutator, mergeEvents: List[SemesterEvent]): Type = new Semester

  /**
    * Get the mutator used for the entity instance.
    * @param entity The instance to get mutator from
    */
  def done(instance: Type) = instance.mutator

}

private[semester] class SemesterMutator
    extends StateMutator {

  type Event = SemesterEvent
  type State = college.semester.State

//  protected def process(evt: SemesterEvent) {
//    evt match {
//      case ClassCreated(name) =>
//        semester = new State(name)
//      case StudentEnrolled(studentId) =>
//        val enrolled = semester.enrolled + studentId
//        semester = semester.copy(enrolled = enrolled)
//      case StudentCancelled(studentId) =>
//        val without = semester.enrolled - studentId
//        semester = semester.copy(enrolled = without)
//    }
//  }
  //  def state = semester
  private def semester = state
  protected val fold = new Fold[State, SemesterEvent] {
    def init(evt: SemesterEvent) = evt match {
      case ClassCreated(name) => new State(name)
    }
    def next(state: State, evt: SemesterEvent) = evt match {
      case StudentEnrolled(studentId) =>
        val enrolled = semester.enrolled + studentId
        semester.copy(enrolled = enrolled)
      case StudentCancelled(studentId) =>
        val without = semester.enrolled - studentId
        semester.copy(enrolled = without)
    }

  }
}

class Semester private (
    private[Semester] val mutator: SemesterMutator = new SemesterMutator) {

//  private def this(state: State) = this(new SemesterMutator(state))

  private def semester = mutator.state

  private def apply(cmd: CreateClass) {
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
