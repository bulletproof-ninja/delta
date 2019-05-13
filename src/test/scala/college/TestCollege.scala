package college

import org.junit._, Assert._

import delta._
import delta.ddd._
import delta.util._
import college.student._
import college.semester._

import scuff._
import scuff.concurrent._
import scala.concurrent._, duration._
import scala.util.{ Random => rand }

import language.implicitConversions
import scala.collection.concurrent.TrieMap
import delta.MessageHubPublishing
import delta.testing.RandomDelayExecutionContext
import scala.util.Success
import scala.util.Failure
import delta.process.MonotonicReplayProcessor
import delta.process.MonotonicJoinState
import delta.process.SnapshotUpdate
import delta.process.ConcurrentMapStore

// FIXME: Something's not right with the test case. Code coverage is incomplete.
class TestCollege {

  implicit def any2fut(unit: Unit): Future[Unit] = Future successful unit

  lazy val eventStore: EventStore[Int, CollegeEvent] =
    new TransientEventStore[Int, CollegeEvent, Array[Byte]](
      RandomDelayExecutionContext, CollegeEventFormat) with MessageHubPublishing[Int, CollegeEvent] {
      def toTopic(ch: Channel) = MessageHub.Topic(ch.toString)
      val txnHub = new LocalHub[TXN](txn => toTopic(txn.channel), RandomDelayExecutionContext)
      val txnChannels = Set(Student.channel, Semester.channel)
      val txnCodec = Codec.noop[TXN]
    }

  implicit def ec = RandomDelayExecutionContext
  lazy val ticker = LamportTicker(eventStore)

  type TXN = eventStore.TXN

  protected var StudentRepository: EntityRepository[Int, StudentEvent, StudentState, Student.Id, student.Student] = _
  protected var SemesterRepository: EntityRepository[Int, SemesterEvent, SemesterState, Semester.Id, semester.Semester] = _

  @Before
  def setup(): Unit = {
    StudentRepository = new EntityRepository(Student, ec)(eventStore, ticker)
    SemesterRepository = new EntityRepository(Semester, ec)(eventStore, ticker)
  }

  private def randomName(): String = (
    rand.nextBetween('A', 'Z' + 1) +: (1 to rand.nextBetween(2, 13)).map(_ => rand.nextBetween('a', 'z' + 1))).mkString

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
        val name = randomName() + " " + (100 + rand.nextBetween(1, 10))
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
        val idx = rand.nextBetween(0, semesterIds.size)
        semesterIds(idx)
      }

    val futureEnrollments = studentIds.flatMap { studentId =>
      val semesters = (1 to rand.nextBetween(1, 11)).map(_ => randomSemester).distinct
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
  def `many-to-many relationship`(): Unit = {
    val Unknown = "<unknown>"
    populate(200, 30)
    val enrollmentQuery = eventStore.query(eventStore.EventSelector(
      Student.channel -> Set(classOf[StudentChangedName], classOf[StudentRegistered]),
      Semester.channel -> Set(classOf[StudentEnrolled]))) _

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
        def studentNameChange(studentName: String, studentId: Student.Id): Unit = {
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
        case Student.channel => onStudent(new Student.Id(txn.stream)) _
        case Semester.channel => onSemester(new Semester.Id(txn.stream)) _
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
  def `with join state`(): Unit = {

    case class SemesterRev(id: Semester.Id, rev: Int) {
      def this(id: Int, rev: Int) = this(new Semester.Id(id), rev)
      override val toString = s"${id.int}:$rev"
    }
    sealed abstract class Model
    case class StudentModel(enrolled: Set[SemesterRev] = Set.empty) extends Model
    case class SemesterModel(students: Set[Student.Id] = Set.empty) extends Model

    class ModelBuilder(memMap: TrieMap[Int, delta.Snapshot[Model]])
      extends MonotonicReplayProcessor[Int, SemesterEvent, Model, Unit](
        10.seconds,
        new ConcurrentMapStore(memMap, None)(_ => Future successful None))
      with MonotonicJoinState[Int, SemesterEvent, Model] {

      protected def join(semesterId: Int, semesterRev: Int, tick: Long, md: Map[String, String], streamState: Option[Model])(evt: SemesterEvent): Map[Int, Processor] =
        preprocessJoin(semesterId, semesterRev, evt)

      private def preprocessJoin(semesterId: Int, semesterRev: Int, evt: SemesterEvent): Map[Int, Processor] = {
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
      protected def processContext(id: Int) = streamPartitions.singleThread(id)
      override protected def onSnapshotUpdate(id: Int, update: SnapshotUpdate) = update match {
        case SnapshotUpdate(Snapshot(StudentModel(enrolled), _, _), true) =>
          println(s"Student $id is currently enrolled in: $enrolled")
        case SnapshotUpdate(Snapshot(SemesterModel(students), rev, _), true) =>
          println(s"Semester $id:$rev currently has enrolled: $students")
        case _ => // Ignore
      }

      protected def process(txn: TXN, currState: Option[Model]) = txn.events.foldLeft(currState getOrElse SemesterModel()) {
        case (model @ SemesterModel(students), StudentEnrolled(studentId)) => model.copy(students = students + studentId)
        case (model @ SemesterModel(students), StudentCancelled(studentId)) => model.copy(students = students - studentId)
        case (model, _) => model
      }

      private def studentEnrolled(semester: SemesterRev)(model: Option[Model]): StudentModel = {
        val student = model collectOrElse new StudentModel
        student.copy(enrolled = student.enrolled + semester)
      }
      private def studentCancelled(semester: SemesterRev)(model: Option[Model]): StudentModel = {
        val student = model.collectAs[StudentModel] getOrElse sys.error("Out of order processing")
        student.copy(enrolled = student.enrolled - semester)
      }
    }

    val (_, semesterIds) = populate(200, 30)
    //    val studentRevs = (Future sequence studentIds.map(id => eventStore.currRevision(id.int).map(rev => id -> rev))).await
    //      .map {
    //        case (id, revOpt) => id -> revOpt.getOrElse{fail(s"Student $id does not exist"); -1}
    //      }.toMap
    val semesterRevs: Map[Semester.Id, Int] = (Future sequence semesterIds.map(id => eventStore.currRevision(id.int).map(rev => id -> rev))).await
      .map {
        case (id, revOpt) => id -> revOpt.getOrElse { fail(s"Semester $id does not exist"); -1 }
      }.toMap
    val inMemoryMap = new TrieMap[Int, Snapshot[Model]]
    val builder = StreamPromise(new ModelBuilder(inMemoryMap))
    eventStore.query(eventStore.Selector(Semester.channel))(builder)
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
