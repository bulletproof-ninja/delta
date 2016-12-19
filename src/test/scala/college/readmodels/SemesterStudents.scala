//package college.readmodels
//
//import ulysses.util.DurableConsumer
//import ulysses.util.CatchUpConsumer
//
//import college._
//import college.student._
//import college.semester._
//
//object SemesterStudents {
//  val UnknownName = "<unknown>"
//  case class StudentState(name: String = UnknownName, semesters: Set[SemesterId] = Set.empty)
//  case class SemesterState(students: Map[StudentId, String])
//}
//
//class SemesterStudents(
//  students: KVStore[Int,
//) extends DurableConsumer[Int, CollegeEvent, String] {
//  protected def selector[T <: ES](es: T): T#Selector = es.EventSelector(Map(
//    Student.channel -> Set(classOf[StudentChangedName], classOf[StudentRegistered]),
//    Semester.channel -> Set(classOf[StudentEnrolled])))
//
//  protected def newConsumer = new CatchUpConsumer[TXN] {
//    def lastProcessedTick: Option[Long] = None
//    def apply(txn: T): Unit
//    def liveConsumer: T => Unit
//
//}
