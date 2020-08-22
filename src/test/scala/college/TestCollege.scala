package college

import org.junit._, Assert._

import delta._
import delta.util._
import delta.testing._

import college.student._
import college.semester._

import scuff._
import scuff.concurrent.{ Threads, StreamPromise, ScuffFutureObject }

import scala.concurrent._, duration._
import scala.util.{ Random => rand }

import scala.collection.concurrent.TrieMap

import delta.MessageTransportPublishing
import delta.testing.RandomDelayExecutionContext
import scala.util.Success
import scala.util.Failure
import delta.process._
import college.validation._
import delta.testing.InMemoryProcStore
import delta.validation.ConsistencyValidation
import delta.read.SnapshotReaderSupport
import delta.read.ReadModel
import delta.read.MessageHubSupport
import java.util.concurrent.ScheduledExecutorService
import college.validation.EmailValidationProcess.State

object TestCollege {

  type Transaction = delta.Transaction[Int, CollegeEvent]

}

// FIXME: Something's not right with the test case. Code coverage is incomplete.
class TestCollege {

  val Scheduler = Threads.newScheduledThreadPool(8, Threads.factory(Threads.SystemThreadGroup), _.printStackTrace)

  protected def initTicker(es: EventSource[Int, CollegeEvent]) = LamportTicker(es)

  private class TransientCollegeEventStore
  extends TransientEventStore(RandomDelayExecutionContext, CollegeEventFormat)(initTicker)
    with MessageTransportPublishing[Int, CollegeEvent]
    with ConsistencyValidation[Int, CollegeEvent] {

      def validationContext(stream: Int) = RandomDelayExecutionContext

      val txChannels = Set(Student.channel, Semester.channel)
      def toTopic(ch: Channel) = MessageTransport.Topic(ch.toString)
      val txTransport = new LocalTransport[Transaction](tx => toTopic(tx.channel), RandomDelayExecutionContext)
      val txCodec = Codec.noop[Transaction]

    }

  def newEventStore(): CollegeEventStore = new TransientCollegeEventStore
  def newEmailIndexUpdateHub(): UpdateHub[Int, Unit] = {
    type Msg = (Int, delta.process.Update[Unit])
    val topic = MessageTransport.Topic("email-index-update")
      def sameTopic(msg: Msg) = topic
    val transport = new delta.util.LocalTransport(sameTopic, RandomDelayExecutionContext)
    MessageHub(transport, topic)
  }

  protected var eventStore: CollegeEventStore = null

  implicit def md = new CollegeMD
  implicit val mdFunc = () => md
  implicit def ec = RandomDelayExecutionContext

  protected var StudentRepository: StudentRepo = null
  protected var SemesterRepository: SemesterRepo = null
  protected var EmailIndexStore: EmailValidationProcessStore = null
  protected var EmailIndexUpdates: UpdateHub[Int, Unit] = null

  @Before
  def setup(): Unit = {

    this.eventStore = newEventStore()

    EmailIndexStore = newEmailValidationProcessStore()
    EmailIndexUpdates = newEmailIndexUpdateHub()
    this.StudentRepository = new CollegeRepo(Student)(eventStore)
    this.SemesterRepository = new CollegeRepo(Semester)(eventStore)

    val emailValidation = new EmailValidationProcess(
      10, EmailIndexStore, Scheduler)(
      StudentRepository, EmailIndexStore, EmailIndexUpdates)

    (emailValidation validate eventStore).await
    eventStore activate emailValidation

  }

  @After
  def after(): Unit = {
    eventStore.ticker.close()
  }

  private def addStudents(count: Int): Seq[Student.Id] = {
    val ids =
      for (_ <- 1 to count) yield {
        val name = randomName
        val student = Student(RegisterStudent(name, EmailAddress(s"$name@school.edu")))
        student(AddStudentEmail(EmailAddress(s"$name@gmail.com")))
        StudentRepository.insert(new Student.Id, student)
      }
    Future.sequence(ids).await
  }

  private def addSemesters(count: Int): Seq[Semester.Id] = {
    val ids =
      for (_ <- 1 to count) yield {
        val name = randomName + " " + (100 + rand.nextBetween(1, 10))
        val cls = Semester(CreateClass(name))
        SemesterRepository.insert(new Semester.Id, cls)
      }
    Future.sequence(ids).await
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
          _._1 apply EnrollStudent(studentId)
        }
      }
    }
    (Future sequence futureEnrollments).await

    val futureUpdates = studentIds.filter(_ => rand.nextFloat() >= 0.5f).flatMap { studentId =>
      val futureEnrollment1 =
        if (rand.nextBoolean()) {
          SemesterRepository.update(randomSemester) {
            _._1 apply EnrollStudent(studentId)
          }
        } else Future.unit
      val futureNameChange = StudentRepository.update(studentId) {
        _._1 apply ChangeStudentName(randomName)
      }
      val futureEnrollment2 = if (rand.nextBoolean()) {
        SemesterRepository.update(randomSemester) {
          _._1 apply EnrollStudent(studentId)
        }
      } else Future.unit
      Seq(futureEnrollment1, futureEnrollment2, futureNameChange)
    }
    (Future sequence futureUpdates).await
    studentIds.toIndexedSeq -> semesterIds.toIndexedSeq
  }

  def newEmailValidationProcessStore(): EmailValidationProcessStore =
    new InMemoryProcStore[Int, State, Unit]("student-emails")
    with EmailValidationProcessStore {

      protected def toQueryValue(addr: EmailAddress) = addr
      protected def emailRefName: String = "emailAddress" // Ref name is irrelevant here
      protected def getEmail: MetaType[EmailAddress] =  (name: String, state: State) => {
        assert(name == emailRefName)
        state.asData.allEmails
      }

      override protected def isQueryMatch(name: String, queryValue: Any, state: State) =
        queryValue match {
          case addr: EmailAddress => state.asData.allEmails contains addr
          case _ => super.isQueryMatch(name, queryValue, state)
        }

    }

  @Test
  def `many-to-many relationship`(): Unit = {

    val eventStore = this.eventStore
    type Transaction = eventStore.Transaction

    case class StudentModel(name: String, emails: Set[String] = Set.empty)

    val Unknown = StudentModel("<unknown>")
    populateRepos(100, 15)
    val enrollmentQuery = eventStore.query(eventStore.ChannelSelector(Student.channel, Semester.channel)) _

    val allStudents = new TrieMap[Student.Id, (Set[Semester.Id], StudentModel)].withDefaultValue(Set.empty -> Unknown)
    val readModel = new TrieMap[Semester.Id, Map[Student.Id, StudentModel]].withDefaultValue(Map.empty)
    val done = StreamPromise.foreach(enrollmentQuery) { tx: Transaction =>
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
          case StudentRegistered(name) =>
            studentNameChange(name, studentId)
          case StudentChangedName(newName) =>
            studentNameChange(newName, studentId)
          case StudentEmailAdded(newEmail) =>
            studentEmailChange(Right(newEmail), studentId)
          case StudentEmailRemoved(removeEmail) =>
            studentEmailChange(Left(removeEmail), studentId)
        }
      val evtHandler = tx.channel match {
        case Student.channel => onStudent(new Student.Id(tx.stream)) _
        case Semester.channel => onSemester(new Semester.Id(tx.stream)) _
      }
      tx.events.foreach(evtHandler)
    }
    done.await
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

    val eventStore = this.eventStore

    case class SemesterRev(id: Semester.Id, rev: Revision) {
      def this(id: Int, rev: Revision) = this(new Semester.Id(id), rev)
      override val toString = s"${id.int}:$rev"
    }
    sealed abstract class Model
    case class StudentModel(enrolled: Set[SemesterRev] = Set.empty) extends Model
    case class SemesterModel(students: Set[Student.Id] = Set.empty) extends Model

    import ConcurrentMapStore.State
    type State = ConcurrentMapStore.State[Model]

    class ModelBuilder(memMap: TrieMap[Int, State])
      extends MonotonicReplayProcessor[Int, SemesterEvent, Model, Model, Unit](
        10.seconds,
        ConcurrentMapStore(memMap, "semester", None)(_ => Future.none))
      with MonotonicJoinState[Int, SemesterEvent, Model, Model] {
      protected def tickWindow = None
      protected def prepareJoin(
          semesterId: Int, semesterRev: Revision, tick: Tick, md: Map[String, String])(
          evt: SemesterEvent): Map[Int, Processor] = {

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
      override protected def onUpdate(id: Int, update: Update) = update match {
        case Update(Some(StudentModel(enrolled)), _, _) =>
          println(s"Student $id is currently enrolled in: $enrolled")
        case Update(Some(SemesterModel(students)), rev, _) =>
          println(s"Semester $id:$rev currently has enrolled: $students")
        case _ => // Ignore
      }

      protected def processStream(tx: Transaction, currState: Option[Model]) =
        tx.events.foldLeft(currState getOrElse SemesterModel()) {
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
    val semesterRevs: Map[Semester.Id, Int] =
      (Future sequence semesterIds.map(id => eventStore.currRevision(id.int).map(rev => id -> rev)))
      .await
      .map {
        case (id, revOpt) => id -> revOpt.getOrElse { fail(s"Semester $id does not exist"); -1 }
      }.toMap
    val inMemoryMap = new TrieMap[Int, State]
    val builder = StreamPromise(new ModelBuilder(inMemoryMap))
    eventStore.query(eventStore.Selector(Semester.channel))(builder)
    builder.future.await
    inMemoryMap.foreach {
      case (semesterId, State(Snapshot(SemesterModel(students), rev, _), _)) =>
        val semId = new Semester.Id(semesterId)
        val expected = semesterRevs.get(semId) match {
          case Some(rev) => rev
          case None =>
            if (semesterIds contains semId) {
              eventStore.currRevision(semId.int).await getOrElse sys.error(s"Semester $semId does not exist in event store, which should be impossible")
            } else fail(s"Semester $semId exists in inMemoryMap, but not in event store. That is basically impossible")
        }
        assertEquals(s"Semester $semesterId revision $rev failed", expected, rev)
        val studentSemesters = students
          .map(id => inMemoryMap(id.int))
          .collect {
            case State(Snapshot(StudentModel(semesters), _, _), _) => semesters.map(_.id.int)
          }
        studentSemesters.foreach { semesters =>
          assertTrue(semesters contains semesterId)
        }
      case (studentId, State(Snapshot(StudentModel(semesters), rev, _), _)) =>
        assertEquals(-1, rev)
        val semesterStudents = semesters
          .map(s => inMemoryMap(s.id.int))
          .collect {
            case State(Snapshot(SemesterModel(students), _, _), _) => students.map(_.int)
          }
        semesterStudents.foreach { students =>
          assertTrue(students contains studentId)
        }
    }
  }

  @Test
  def `lookup by email`(): Unit = {

    type Snapshot = delta.Snapshot[Unit]
    type Update = delta.process.Update[Unit]

      def minRevision(revision: Int): PartialFunction[Either[Snapshot, Update], Unit] = {
        case either
          if either.map(_.revision).left.map(_.revision).merge >= revision =>
            ()
      }

    val emailIndexModel =
      new ReadModel[Student.Id, Unit]
      with SnapshotReaderSupport[Student.Id, Unit]
      with MessageHubSupport[Student.Id, Unit, Unit] {
        protected type StreamId = Int
        protected def StreamId(id: Student.Id): StreamId = id.int
        protected val snapshotReader = EmailIndexStore.asSnapshotReader[Unit]

        private[this] val SomeUnit = Some(())
        protected def updateState(
            id: Student.Id, prevState: Option[Unit], update: Unit) =
          SomeUnit
        protected val defaultReadTimeout: FiniteDuration = 10.seconds
        protected def scheduler: ScheduledExecutorService = Scheduler
        protected def hub: MessageHub[StreamId, Update] = EmailIndexUpdates
    }

    val emailA = EmailAddress("A@school.edu")
    val studentA = Student(RegisterStudent("A", emailA))
    val idA = StudentRepository.insert(new Student.Id, studentA).await

    val emailB = EmailAddress("B@school.edu")
    val studentB = Student(RegisterStudent("B", emailB))
    val idB = StudentRepository.insert(new Student.Id, studentB).await

    val dupes = EmailIndexStore.findDuplicates().await
    assertTrue(s"Duplicates found: $dupes", dupes.isEmpty)

    emailIndexModel.readCustom(idA)(minRevision(0)).await
    assertEquals(idA, EmailIndexStore.lookup(emailA).await.get)

    emailIndexModel.read(idB, minRevision = 0).await
    assertEquals(idB, EmailIndexStore.lookup(emailB).await.get)

    val emailA2 = EmailAddress("a@SCHOOL.EDU")
    val emailB2 = EmailAddress("b@SCHOOL.EDU")
    assertEquals(idA, EmailIndexStore.lookup(emailA2).await.get)
    assertEquals(idB, EmailIndexStore.lookup(emailB2).await.get)

    StudentRepository.update(idA) {
      case (student, 0) =>
        student apply AddStudentEmail(emailB2)
      case (_, rev) =>
        sys.error(s"Unexpected revision: $rev")
    }.await
    assertEquals(idB, EmailIndexStore.lookup(emailB2).await.get)

    emailIndexModel.readCustom(idA)(minRevision(1)).await
    assertEquals(idB, EmailIndexStore.lookup(emailB2).await.get)

    val snapshot = emailIndexModel.read(idA, minRevision = 2).await
    assertEquals(2, snapshot.revision)
    assertEquals(1, EmailIndexStore.lookupAll(emailB2).await.size)

  }
}
