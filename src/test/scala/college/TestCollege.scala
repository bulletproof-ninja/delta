package college

import org.junit._, Assert._

import delta._
import delta.ddd._
import delta.util._
//import scala.concurrent.ExecutionContext
import college.student._
import college.semester._

import scuff.ScuffRandom
import scuff.concurrent._
import scala.concurrent._, duration._
import scala.util.{ Random => rand }

import language.implicitConversions
import scala.collection.concurrent.TrieMap
import delta.util.LocalPublishing
import delta.testing.RandomDelayExecutionContext
import scala.reflect.ClassTag

// FIXME: Something's not right with the test case. Code coverage is incomplete.
class TestCollege {

  implicit def any2fut(unit: Unit): Future[Unit] = Future successful unit

  lazy val eventStore: EventStore[Int, CollegeEvent, String] =
    new TransientEventStore[Int, CollegeEvent, String, Array[Byte]](
      RandomDelayExecutionContext) with LocalPublishing[Int, CollegeEvent, String] {
      def publishCtx = RandomDelayExecutionContext
    }

  implicit def ec = RandomDelayExecutionContext
  implicit lazy val ticker = LamportTicker(eventStore)

  type TXN = eventStore.TXN

  protected var StudentRepository: EntityRepository[Int, StudentEvent, String, student.State, StudentId, student.Student] = _
  protected var SemesterRepository: EntityRepository[Int, SemesterEvent, String, semester.State, SemesterId, semester.Semester] = _

  @Before
  def setup() {
    StudentRepository = new EntityRepository("Student", student.Student)(eventStore)
    SemesterRepository = new EntityRepository("Semester", semester.Semester)(eventStore)
  }

  private def randomName(): String = (
    rand.nextInRange('A' to 'Z') +: (1 to rand.nextInRange(2 to 12)).map(_ => rand.nextInRange('a' to 'z'))).mkString

  // Count not exact, since we can potentially generate id clashes
  private def addStudents(approx: Int): Seq[StudentId] = {
    val ids =
      for (_ <- 1 to approx) yield {
        val name = randomName()
        val id = new StudentId(rand.nextInt)
        val student = Student(RegisterStudent(name))
        StudentRepository.insert(id, student).map(_ => id)
      }
    Future.sequence(ids).await(60.seconds)
  }
  // Count not exact, since we can potentially generate id clashes
  private def addSemesters(approx: Int): Seq[SemesterId] = {
    val ids =
      for (_ <- 1 to approx) yield {
        val name = randomName() + " " + (100 + rand.nextInRange(1 to 9))
        val id = new SemesterId(rand.nextInt)
        val cls = Semester(CreateClass(name))
        SemesterRepository.insert(id, cls).map(_ => id)
      }
    Future.sequence(ids).await
  }

  private def populate(studentCount: Int, semesterCount: Int): (Seq[StudentId], Seq[SemesterId]) = {
    val studentIds = addStudents(studentCount)
    val semesterIds = addSemesters(semesterCount)
      def randomSemester: SemesterId = {
        val idx = rand.nextInRange(0 until semesterIds.size)
        semesterIds(idx)
      }
    studentIds.foreach { studentId =>
      val semesters = (1 to rand.nextInRange(1 to 10)).map(_ => randomSemester).distinct
      semesters.foreach { semesterId =>
        SemesterRepository.update(semesterId) {
          case (semester, _) =>
            semester(EnrollStudent(studentId))
        }
      }
    }
    studentIds.filter(_ => rand.nextFloat >= 0.5f).foreach { studentId =>
      if (rand.nextBoolean) {
        SemesterRepository.update(randomSemester) {
          case (semester, _) =>
            semester(EnrollStudent(studentId))
        }
      }
      StudentRepository.update(studentId) {
        case (student, _) =>
          student(ChangeStudentName(randomName))
      }
      if (rand.nextBoolean) {
        SemesterRepository.update(randomSemester) {
          case (semester, _) =>
            semester(EnrollStudent(studentId))
        }
      }
    }
    studentIds.toIndexedSeq -> semesterIds.toIndexedSeq
  }

  @Test
  def `many-to-many relationship`() {
    val Unknown = "<unknown>"
    val (studentIds, semesterIds) = populate(200, 30)
      def randomSemester: SemesterId = {
        val idx = rand.nextInRange(0 until semesterIds.size)
        semesterIds(idx)
      }
    studentIds.foreach { studentId =>
      val semesters = (1 to rand.nextInRange(1 to 10)).map(_ => randomSemester).distinct
      semesters.foreach { semesterId =>
        SemesterRepository.update(semesterId) {
          case (semester, _) =>
            semester(EnrollStudent(studentId))
        }
      }
    }
    studentIds.filter(_ => rand.nextFloat >= 0.5f).foreach { studentId =>
      if (rand.nextBoolean) {
        SemesterRepository.update(randomSemester) {
          case (semester, _) =>
            semester(EnrollStudent(studentId))
        }
      }
      StudentRepository.update(studentId) {
        case (student, _) =>
          student(ChangeStudentName(randomName))
      }
      if (rand.nextBoolean) {
        SemesterRepository.update(randomSemester) {
          case (semester, _) =>
            semester(EnrollStudent(studentId))
        }
      }
    }
    val enrollmentQuery = eventStore.query(eventStore.EventSelector(Map(
      "Student" -> Set(classOf[StudentChangedName], classOf[StudentRegistered]),
      "Semester" -> Set(classOf[StudentEnrolled])))) _

    val allStudents = new TrieMap[StudentId, (Set[SemesterId], String)].withDefaultValue(Set.empty -> Unknown)
    val readModel = new TrieMap[SemesterId, Map[StudentId, String]].withDefaultValue(Map.empty)
    val done = StreamPromise.foreach(enrollmentQuery) { txn: TXN =>
        def onSemester(semesterId: SemesterId)(evt: CollegeEvent) = evt match {
          case StudentEnrolled(studentId) =>
            val semesterStudents = readModel(semesterId)
            val (studentSemesters, studentName) = allStudents(studentId)
            allStudents.update(studentId, (studentSemesters + semesterId, studentName))
            readModel.update(semesterId, semesterStudents.updated(studentId, studentName))
        }
        def studentNameChange(studentName: String, studentId: StudentId) {
          val (studentSemesters, _) = allStudents(studentId)
          allStudents.update(studentId, (studentSemesters, studentName))
          studentSemesters.foreach { semesterId =>
            val semesterStudents = readModel(semesterId)
            readModel.update(semesterId, semesterStudents.updated(studentId, studentName))
          }
        }
        def onStudent(studentId: StudentId)(evt: CollegeEvent) = evt match {
          case StudentRegistered(studentName) => studentNameChange(studentName, studentId)
          case StudentChangedName(newName) => studentNameChange(newName, studentId)
        }
      val evtHandler = txn.channel match {
        case "Student" => onStudent(new StudentId(txn.stream)) _
        case "Semester" => onSemester(new SemesterId(txn.stream)) _
      }
      txn.events.foreach(evtHandler)
    }
    done.await(60.seconds)
    allStudents.values.foreach {
      case (_, name) => assertNotEquals(Unknown, name)
    }
    readModel.foreach {
      case (semesterId, students) =>
        students.foreach {
          case (studentId, name) =>
            val (studentSemesters, studentName) = allStudents(studentId)
            assertEquals(studentName, name)
            assertTrue(studentSemesters.contains(semesterId))
        }
    }
  }

  @Test
  def `indirect model`() {

    sealed abstract class Model
    case class StudentModel(enrolled: Set[SemesterId]) extends Model
    case object SemesterModelIsEmpty extends Model

    class ModelBuilder(memMap: TrieMap[Int, Option[delta.Snapshot[Model]]])
      extends MonotonicProcessor[Int, SemesterEvent, Model]
      with CollateralState[Int, SemesterEvent, Model] {

      def evtTag = implicitly[ClassTag[SemesterEvent]]

      protected def preprocess(streamId: Int, streamRevision: Int, tick: Long, evt: SemesterEvent): Map[Int, Processor] = evt match {
        case StudentEnrolled(studentId) => Map {
          studentId.int -> Processor(studentEnrolled(new SemesterId(streamId)) _)
        }
        case StudentCancelled(studentId) => Map {
          studentId.int -> Processor(studentCancelled(new SemesterId(streamId)) _)
        }
        case _ => Map.empty
      }
      protected val processStore = new ConcurrentMapStore(memMap, (_: Int) => Future successful None)
      import processStore.Update
      private val streamPartitions = scuff.concurrent.PartitionedExecutionContext(3)
      protected def executionContext(id: Int) = streamPartitions.singleThread(id)
      protected def onMissingRevisions(id: Int, missing: Range) = ()
      protected def onUpdate(id: Int, update: Update) = update match {
        case Update(Snapshot(StudentModel(enrolled), _, _), true) =>
          println(s"Student $id is currently enrolled in: $enrolled")
        case _ => // Ignore
      }

      protected def process(txn: TXN, currState: Option[Model]) = SemesterModelIsEmpty

      private def studentEnrolled(semesterId: SemesterId)(model: Option[Model]): StudentModel =
        model match {
          case Some(model @ StudentModel(enrolled)) => model.copy(enrolled = enrolled + semesterId)
          case None => new StudentModel(Set(semesterId))
          case _ => sys.error("Should never happen")
        }
      private def studentCancelled(semesterId: SemesterId)(model: Option[Model]): StudentModel =
        model match {
          case Some(model @ StudentModel(enrolled)) => model.copy(enrolled = enrolled - semesterId)
          case None => sys.error("Out of order processing")
          case _ => sys.error("Should never happen")
        }

    }

    val (studentIds, semesterIds) = populate(200, 30)
    val inMemoryMap = new TrieMap[Int, Option[Snapshot[Model]]]
    val builder = new ModelBuilder(inMemoryMap)
    val semesterQuery = eventStore.query(eventStore.Selector("Semester")) _
    val queryFinished = StreamPromise.foreach(semesterQuery)(builder)
  }
}
