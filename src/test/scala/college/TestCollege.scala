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
import delta.process.StreamProcessStore
import delta.process.PersistentMonotonicConsumer
import delta.process.BlockingCASWrites
import delta.process.StreamProcessStore

object TestCollege {

  case class StudentEmails(emails: Set[String])

}

// FIXME: Something's not right with the test case. Code coverage is incomplete.
class TestCollege {

  val Scheduler = Threads.newScheduledThreadPool(8, Threads.factory(Threads.SystemThreadGroup), _.printStackTrace)
  
  import TestCollege._

  implicit def any2fut(unit: Unit): Future[Unit] = Future successful unit

  lazy val eventStore: EventStore[Int, CollegeEvent] =
    new TransientEventStore[Int, CollegeEvent, Array[Byte]](
      RandomDelayExecutionContext, CollegeEventFormat) with MessageHubPublishing[Int, CollegeEvent] {
      def toTopic(ch: Channel) = MessageHub.Topic(ch.toString)
      val txnHub = new LocalHub[TXN](txn => toTopic(txn.channel), RandomDelayExecutionContext)
      val txnChannels = Set(Student.channel, Semester.channel)
      val txnCodec = Codec.noop[TXN]
    }

  implicit val md = Metadata.empty
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
        val student = Student(RegisterStudent(name, EmailAddress(s"$name@school.edu")))
        student(AddStudentEmail(EmailAddress(s"$name@gmail.com")))
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

  private def populateRepos(studentCount: Int, semesterCount: Int): (Seq[Student.Id], Seq[Semester.Id]) = {
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

  def newLookupServiceProcStore: StreamProcessStore[Int, StudentEmails] = InMemoryProcStore(new TrieMap)

  def lookupService(procStore: StreamProcessStore[Int, StudentEmails]): LookupService = procStore match {
    case InMemoryProcStore(snapshots) =>
      new LookupService {
        def findStudent(email: EmailAddress): Future[Option[Student.Id]] = Future {
          val lowerCase = email.toLowerCase
          val result = snapshots.mapValues(_.content.emails).find {
            case (_, emails) => emails contains lowerCase
          }
          result.map(_._1).map(new Student.Id(_))
        }
      }
    case _ => sys.error("This method should be overwritten when `def procStore[S]` is overwritten.")
  }

  private case class InMemoryProcStore[S](snapshots: collection.concurrent.Map[Int, delta.Snapshot[S]])
    extends StreamProcessStore[Int, S] with BlockingCASWrites[Int, S, Unit] {
    def read(key: Int): scala.concurrent.Future[Option[Snapshot]] = Future {
      snapshots.get(key)
    }
    def write(key: Int, snapshot: Snapshot): scala.concurrent.Future[Unit] = Future {
      snapshots.update(key, snapshot)
    }

    def readBatch(keys: Iterable[Int]): Future[Map[Int, Snapshot]] = Future {
      snapshots.filterKeys(keys.toSet).toMap
    }
    def writeBatch(batch: scala.collection.Map[Int, Snapshot]): Future[Unit] = Future {
      batch.foreach {
        case (key, snapshot) => snapshots.update(key, snapshot)
      }
    }
    def refresh(key: Int, revision: Int, tick: Long): Future[Unit] = Future {
      refreshKey(())(key, revision, tick)
    }
    def refreshBatch(revisions: scala.collection.Map[Int, (Int, Long)]): Future[Unit] = Future {
      revisions.foreach {
        case (key, (revision, tick)) =>
          snapshots.updateIfPresent(key)(_.copy(tick = tick, revision = revision))
      }
    }
    def tickWatermark: Option[Long] = None
    protected def readForUpdate[R](key: Int)(thunk: (Unit, Option[Snapshot]) => R): R =
      thunk((), snapshots.get(key))

    protected def refreshKey(conn: Unit)(key: Int, revision: Int, tick: Long): Unit =
      snapshots.updateIfPresent(key)(_.copy(tick = tick, revision = revision))

    protected def writeIfAbsent(conn: Unit)(key: Int, snapshot: Snapshot): Option[Snapshot] =
      snapshots.putIfAbsent(key, snapshot)

    protected def writeReplacement(conn: Unit)(key: Int, oldSnapshot: Snapshot, newSnapshot: Snapshot): Option[Snapshot] =
      if (snapshots.replace(key, oldSnapshot, newSnapshot)) None
      else snapshots.get(key) match {
        case None => snapshots.putIfAbsent(key, newSnapshot)
        case existing => existing
      }

    protected def blockingCtx = RandomDelayExecutionContext

  }

  object StudentEmailsProjector extends Projector[StudentEmails, StudentEvent] {
    def init(evt: StudentEvent): StudentEmails = next(null, evt)
    def next(model: StudentEmails, evt: StudentEvent): StudentEmails = evt match {

      case StudentRegistered(_, email) =>
        assert(model == null); StudentEmails(Set(email))

      case StudentEmailAdded(email) => model.copy(model.emails + email)
      case StudentEmailRemoved(email) => model.copy(model.emails - email)

      case _ => model
    }
  }

  @Test
  def `many-to-many relationship`(): Unit = {

    case class StudentModel(name: String, emails: Set[String] = Set.empty)

    val Unknown = StudentModel("<unknown>")
    populateRepos(100, 15)
    val enrollmentQuery = eventStore.query(eventStore.ChannelSelector(Student.channel, Semester.channel)) _

    val allStudents = new TrieMap[Student.Id, (Set[Semester.Id], StudentModel)].withDefaultValue(Set.empty -> Unknown)
    val readModel = new TrieMap[Semester.Id, Map[Student.Id, StudentModel]].withDefaultValue(Map.empty)
    val done = StreamPromise.foreach(enrollmentQuery) { txn: TXN =>
        def onSemester(semesterId: Semester.Id)(evt: CollegeEvent) = evt match {
          case ClassCreated(_) => ()
          case StudentEnrolled(studentId) =>
            val semesterStudents = readModel(semesterId)
            val (studentSemesters, studentModel) = allStudents(studentId)
            allStudents.update(studentId, (studentSemesters + semesterId, studentModel))
            readModel.update(semesterId, semesterStudents.updated(studentId, studentModel))
        }
        def studentNameChange(studentName: String, studentId: Student.Id): Unit = {
          val (studentSemesters, oldStudent) = allStudents(studentId)
          val studentModel = oldStudent.copy(name = studentName)
          allStudents.update(studentId, (studentSemesters, studentModel))
          studentSemesters.foreach { semesterId =>
            val semesterStudents = readModel(semesterId)
            readModel.update(semesterId, semesterStudents.updated(studentId, studentModel))
          }
        }
        def studentEmailChange(email: Either[String, String], studentId: Student.Id): Unit = {
          val (studentSemesters, oldStudent) = allStudents(studentId)
          val studentModel = email match {
            case Left(remove) => oldStudent.copy(emails = oldStudent.emails - remove)
            case Right(add) => oldStudent.copy(emails = oldStudent.emails + add)
          }
          allStudents.update(studentId, (studentSemesters, studentModel))
          studentSemesters.foreach { semesterId =>
            val semesterStudents = readModel(semesterId)
            readModel.update(semesterId, semesterStudents.updated(studentId, studentModel))
          }
        }
        def onStudent(studentId: Student.Id)(evt: CollegeEvent) = evt match {
          case StudentRegistered(name, email) =>
            studentNameChange(name, studentId)
            studentEmailChange(Right(email), studentId)
          case StudentChangedName(newName) =>
            studentNameChange(newName, studentId)
          case StudentEmailAdded(newEmail) =>
            studentEmailChange(Right(newEmail), studentId)
          case StudentEmailRemoved(removeEmail) =>
            studentEmailChange(Left(removeEmail), studentId)
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

      val streamPartitions = scuff.concurrent.PartitionedExecutionContext(1, _.printStackTrace(System.err))
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

    val (_, semesterIds) = populateRepos(100, 20)
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

  @Test
  def `lookup by email`(): Unit = {
    val procStore = newLookupServiceProcStore
    val lookup = lookupService(procStore)
    object ServiceBuilder
      extends PersistentMonotonicConsumer[Int, StudentEvent, StudentEmails](procStore, Scheduler) {
      protected def maxTickSkew: Int = 2
      protected def selector(es: EventSource): es.Selector = es.ChannelSelector(Student.channel)
      protected def onSnapshotUpdate(id: Int, update: SnapshotUpdate): Unit = ()
      protected def reportFailure(th: Throwable): Unit = {
        th.printStackTrace()
        fail(th.getMessage)
      }
      protected def process(txn: TXN, currState: Option[StudentEmails]): StudentEmails = {
        Projector.process(StudentEmailsProjector)(currState, txn.events)
      }
    }
    val subscription = ServiceBuilder.consume(eventStore).await
    assertEquals(None, lookup.findStudent(EmailAddress("foo@bar.com")).await)
    val students = addStudents(200)

      def verifyLookup(studentId: Student.Id, expectedDomain: String, unexpectedDomain: String): Unit = {
        val (student, _) = StudentRepository.load(studentId).await
        assertTrue(student.state.curr.emails.exists(_ endsWith expectedDomain))
        assertFalse(student.state.curr.emails.exists(_ endsWith unexpectedDomain))
        student.state.curr.emails.foreach { email =>
          val id = lookup.findStudent(EmailAddress(email)).await.get
          assertEquals(studentId, id)
        }
      }

    val updates = students.map { studentId =>
      verifyLookup(studentId, "gmail.com", "aol.com")
      // going old school
      studentId -> StudentRepository.update(studentId) {
        case (student, _) =>
          val oldEmail = EmailAddress(student.state.curr.emails.find(_ contains "gmail.com").get)
          student(RemoveStudentEmail(oldEmail))
          val newEmail = EmailAddress(oldEmail.user, "aol.com")
          student(AddStudentEmail(newEmail))
      }
    }
    updates.foreach {
      case (studentId, update) =>
        update.await
        verifyLookup(studentId, "aol.com", "gmail.com")
    }
    assertEquals(None, lookup.findStudent(EmailAddress("foo@bar.com")).await)
    subscription.cancel()
  }

}
