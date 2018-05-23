package college

import org.junit._, Assert._

import delta._
import delta.ddd._
import delta.util._
import college.student._
import college.semester._

import scuff.ScuffRandom
import scuff.concurrent._
import scala.concurrent._, duration._
import scala.util.{ Random => rand }

import language.implicitConversions
import scala.collection.concurrent.TrieMap
import delta.Publishing
import delta.testing.RandomDelayExecutionContext
import scala.util.Success
import scala.util.Failure

// FIXME: Something's not right with the test case. Code coverage is incomplete.
class TestCollege {

  implicit def any2fut(unit: Unit): Future[Unit] = Future successful unit

  lazy val eventStore: EventStore[Int, CollegeEvent] =
    new TransientEventStore[Int, CollegeEvent, Array[Byte]](
      RandomDelayExecutionContext) with Publishing[Int, CollegeEvent] {
      val publisher = new LocalPublisher[Int, CollegeEvent](RandomDelayExecutionContext)
    }

  implicit def ec = RandomDelayExecutionContext
  implicit lazy val ticker = LamportTicker(eventStore)

  type TXN = eventStore.TXN

  protected var StudentRepository: EntityRepository[Int, StudentEvent, StudentState, Student.Id, student.Student] = _
  protected var SemesterRepository: EntityRepository[Int, SemesterEvent, SemesterState, Semester.Id, semester.Semester] = _

  @Before
  def setup() {
    StudentRepository = new EntityRepository(Student)(eventStore)
    SemesterRepository = new EntityRepository(Semester)(eventStore)
  }

  private def randomName(): String = (
    rand.nextInRange('A' to 'Z') +: (1 to rand.nextInRange(2 to 12)).map(_ => rand.nextInRange('a' to 'z'))).mkString

  // Count not exact, since we can potentially generate id clashes
  private def addStudents(approx: Int): Seq[Student.Id] = {
    val ids =
      for (_ <- 1 to approx) yield {
        val name = randomName()
        val id = new Student.Id(rand.nextInt)
        val student = Student(RegisterStudent(name))
        StudentRepository.insert(id, student).map(_ => id)
      }
    Future.sequence(ids).await(60.seconds)
  }
  // Count not exact, since we can potentially generate id clashes
  private def addSemesters(approx: Int): Seq[Semester.Id] = {
    val ids =
      for (_ <- 1 to approx) yield {
        val name = randomName() + " " + (100 + rand.nextInRange(1 to 9))
        val id = new Semester.Id(rand.nextInt)
        val cls = Semester(CreateClass(name))
        SemesterRepository.insert(id, cls).map(_ => id)
      }
    Future.sequence(ids).await(120.seconds)
  }

  private def populate(studentCount: Int, semesterCount: Int): (Seq[Student.Id], Seq[Semester.Id]) = {
    val studentIds = addStudents(studentCount)
    val semesterIds = addSemesters(semesterCount)
      def randomSemester: Semester.Id = {
        val idx = rand.nextInRange(0 until semesterIds.size)
        semesterIds(idx)
      }

    val futureEnrollments = studentIds.flatMap { studentId =>
      val semesters = (1 to rand.nextInRange(1 to 10)).map(_ => randomSemester).distinct
      semesters.map { semesterId =>
        SemesterRepository.update(semesterId) {
          case (semester, _) =>
            semester(EnrollStudent(studentId))
        }
      }
    }
    (Future sequence futureEnrollments).await(360.seconds)

    val futureUpdates = studentIds.filter(_ => rand.nextFloat >= 0.5f).flatMap { studentId =>
      val futureEnrollment1 =
        if (rand.nextBoolean) {
          SemesterRepository.update(randomSemester) {
            case (semester, _) =>
              semester(EnrollStudent(studentId))
          }
        } else Future successful (())
      val futureNameChange = StudentRepository.update(studentId) {
        case (student, _) =>
          student(ChangeStudentName(randomName))
      }
      val futureEnrollment2 = if (rand.nextBoolean) {
        SemesterRepository.update(randomSemester) {
          case (semester, _) =>
            semester(EnrollStudent(studentId))
        }
      } else Future successful (())
      Seq(futureEnrollment1, futureEnrollment2, futureNameChange)
    }
    (Future sequence futureUpdates).await(360.seconds)
    studentIds.toIndexedSeq -> semesterIds.toIndexedSeq
  }

  @Test
  def `many-to-many relationship`() {
    val Unknown = "<unknown>"
    populate(200, 30)
    val enrollmentQuery = eventStore.query(eventStore.EventSelector(
      Student.name -> Set(classOf[StudentChangedName], classOf[StudentRegistered]),
      Semester.name -> Set(classOf[StudentEnrolled]))) _

    val allStudents = new TrieMap[Student.Id, (Set[Semester.Id], String)].withDefaultValue(Set.empty -> Unknown)
    val readModel = new TrieMap[Semester.Id, Map[Student.Id, String]].withDefaultValue(Map.empty)
    val done = StreamPromise.foreach(enrollmentQuery) { txn: TXN =>
        def onSemester(semesterId: Semester.Id)(evt: CollegeEvent) = evt match {
          case StudentEnrolled(studentId) =>
            val semesterStudents = readModel(semesterId)
            val (studentSemesters, studentName) = allStudents(studentId)
            allStudents.update(studentId, (studentSemesters + semesterId, studentName))
            readModel.update(semesterId, semesterStudents.updated(studentId, studentName))
        }
        def studentNameChange(studentName: String, studentId: Student.Id) {
          val (studentSemesters, _) = allStudents(studentId)
          allStudents.update(studentId, (studentSemesters, studentName))
          studentSemesters.foreach { semesterId =>
            val semesterStudents = readModel(semesterId)
            readModel.update(semesterId, semesterStudents.updated(studentId, studentName))
          }
        }
        def onStudent(studentId: Student.Id)(evt: CollegeEvent) = evt match {
          case StudentRegistered(studentName) => studentNameChange(studentName, studentId)
          case StudentChangedName(newName) => studentNameChange(newName, studentId)
        }
      val evtHandler = txn.channel match {
        case Student.name => onStudent(new Student.Id(txn.stream)) _
        case Semester.name => onSemester(new Semester.Id(txn.stream)) _
      }
      txn.events.foreach(evtHandler)
    }
    done.await(120.seconds)
    allStudents.foreach {
      case (id, (_, name)) => assertNotEquals(s"Student $id name was not updated", Unknown, name)
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
  def `with join state`() {

    case class SemesterRev(id: Semester.Id, rev: Int) {
      def this(id: Int, rev: Int) = this(new Semester.Id(id), rev)
      override val toString = s"${id.int}:$rev"
    }
    sealed abstract class Model
    case class StudentModel(enrolled: Set[SemesterRev]) extends Model
    case class SemesterModel(students: Set[Student.Id] = Set.empty) extends Model

    class ModelBuilder(memMap: TrieMap[Int, delta.Snapshot[Model]])
      extends MonotonicBatchProcessor[Int, SemesterEvent, Model, Unit](
          10.seconds,
          new ConcurrentMapStore(memMap)(_ => Future successful None))
      with JoinState[Int, SemesterEvent, Model] {

      type JoinState = StudentModel

      protected def preprocess(semesterId: Int, semesterRev: Int, tick: Long, evt: SemesterEvent): Map[Int, Processor] = {
        val semester = new SemesterRev(semesterId, semesterRev)
        evt match {
          case StudentEnrolled(studentId) => Map {
            studentId.int -> Processor(studentEnrolled(semester) _)
          }
          case StudentCancelled(studentId) => Map {
            studentId.int -> Processor(studentCancelled(semester) _)
          }
          case _ => Map.empty
        }
      }

      val streamPartitions = scuff.concurrent.PartitionedExecutionContext(1, th => th.printStackTrace(System.err))
      def whenDone(): Future[Unit] = {
        println("Shutting down stream partitions...")
        streamPartitions.shutdown().andThen {
          case Success(_) => println("Stream partitions shut down successfully.")
          case Failure(th) => th.printStackTrace(System.err)
        }
      }
      protected def executionContext(id: Int) = streamPartitions.singleThread(id)
      override protected def onUpdate(id: Int, update: Update) = update match {
//        case Update(Snapshot(StudentModel(enrolled), _, _), true) =>
//          println(s"Student $id is currently enrolled in: $enrolled")
//        case Update(Snapshot(SemesterModel(students), rev, _), true) =>
//          println(s"Semester $id:$rev currently has enrolled: $students")
        case _ => // Ignore
      }

      protected def process(txn: TXN, currState: Option[Model]) = txn.events.foldLeft(currState getOrElse SemesterModel()) {
        case (model @ SemesterModel(students), StudentEnrolled(studentId)) => model.copy(students = students + studentId)
        case (model @ SemesterModel(students), StudentCancelled(studentId)) => model.copy(students = students - studentId)
        case (model, _) => model
      }

      private def studentEnrolled(semester: SemesterRev)(model: Option[StudentModel]): StudentModel =
        model match {
          case Some(model @ StudentModel(enrolled)) => model.copy(enrolled = enrolled + semester)
          case None => new StudentModel(Set(semester))
          case _ => sys.error("Should never happen")
        }
      private def studentCancelled(semester: SemesterRev)(model: Option[StudentModel]): StudentModel =
        model match {
          case Some(model @ StudentModel(enrolled)) => model.copy(enrolled = enrolled - semester)
          case None => sys.error("Out of order processing")
          case _ => sys.error("Should never happen")
        }

    }

    val (_, semesterIds) = populate(200, 30)
//    val studentRevs = (Future sequence studentIds.map(id => eventStore.currRevision(id.int).map(rev => id -> rev))).await
//      .map {
//        case (id, revOpt) => id -> revOpt.getOrElse{fail(s"Student $id does not exist"); -1}
//      }.toMap
    val semesterRevs: Map[Semester.Id, Int] = (Future sequence semesterIds.map(id => eventStore.currRevision(id.int).map(rev => id -> rev))).await
      .map {
        case (id, revOpt) => id -> revOpt.getOrElse{fail(s"Semester $id does not exist"); -1}
      }.toMap
    val inMemoryMap = new TrieMap[Int, Snapshot[Model]]
    val builder = StreamPromise(new ModelBuilder(inMemoryMap))
    eventStore.query(eventStore.Selector("Semester"))(builder)
//    val queryProcess = StreamPromise.foreach(semesterQuery)(builder)
    builder.future.await
    inMemoryMap.foreach {
      case (semesterId, Snapshot(SemesterModel(students), rev, _)) =>
        assertEquals(s"Semester $semesterId revision $rev failed", semesterRevs.get(new Semester.Id(semesterId)), Some(rev))
        val studentSemesters = students
          .map(id => inMemoryMap(id.int))
          .collect {
            case Snapshot(StudentModel(semesters), _, _) => semesters.map(_.id.int)
          }
        studentSemesters.foreach { semesters =>
          assertTrue(semesters contains semesterId)
        }
      case (studentId, Snapshot(StudentModel(semesters), rev, _)) =>
        assertEquals(-1, rev)
        val semesterStudents = semesters
          .map(s => inMemoryMap(s.id.int))
          .collect {
            case Snapshot(SemesterModel(students), _, _) => students.map(_.int)
          }
        semesterStudents.foreach { students =>
          assertTrue(students contains studentId)
        }
    }
  }
}
