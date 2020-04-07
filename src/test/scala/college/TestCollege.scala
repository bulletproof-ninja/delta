package college

import org.junit._, Assert._

import delta._
import delta.write._
import delta.util._
import college.student._
import college.semester._

import scuff._
import scuff.concurrent._
import scala.concurrent._, duration._
import scala.util.{ Random => rand }

import scala.collection.concurrent.TrieMap
import delta.MessageTransportPublishing
import delta.testing.RandomDelayExecutionContext
import scala.util.Success
import scala.util.Failure
import delta.process._

object TestCollege {

  case class StudentEmails(emails: Set[String])
  case class StudentEmailsUpdate(added: Set[String] = Set.empty, removed: Set[String] = Set.empty)

  implicit object StudentEmailsUpdateCodec extends UpdateCodec[StudentEmails, StudentEmailsUpdate] {
    def asUpdate(
        prevState: Option[TestCollege.StudentEmails],
        currState: TestCollege.StudentEmails)
        : TestCollege.StudentEmailsUpdate = {

      val origEmails = prevState.map(_.emails) getOrElse Set.empty
      val diff = origEmails diff currState.emails
      val (removed, added) = diff.partition(origEmails.contains)
      StudentEmailsUpdate(added = added, removed = removed)
    }

    def asSnapshot(
        state: Option[TestCollege.StudentEmails],
        update: TestCollege.StudentEmailsUpdate)
        : Option[TestCollege.StudentEmails] = Some {

      val emails = state.map(_.emails) getOrElse Set.empty
      StudentEmails((emails -- update.removed) ++ update.added)
    }
  }

}

// FIXME: Something's not right with the test case. Code coverage is incomplete.
class TestCollege {

  val Scheduler = Threads.newScheduledThreadPool(8, Threads.factory(Threads.SystemThreadGroup), _.printStackTrace)

  import TestCollege._

  implicit def toFuture[T](t: T): Future[T] = Future successful t
  protected def initTicker(es: EventSource[Int, CollegeEvent]) = LamportTicker(es)

  def newEventStore: EventStore[Int, CollegeEvent] =
    new TransientEventStore(RandomDelayExecutionContext, CollegeEventFormat)(initTicker)
        with MessageTransportPublishing[Int, CollegeEvent] {
      def toTopic(ch: Channel) = MessageTransport.Topic(ch.toString)
      val txTransport = new LocalTransport[Transaction](tx => toTopic(tx.channel), RandomDelayExecutionContext)
      val txChannels = Set(Student.channel, Semester.channel)
      val txCodec = Codec.noop[Transaction]
    }

  @volatile var eventStore: EventStore[Int, CollegeEvent] = null

  implicit val md = Metadata.empty
  implicit def ec = RandomDelayExecutionContext

  protected var StudentRepository: EntityRepository[Int, StudentEvent, StudentState, Student.Id, student.Student] = _
  protected var SemesterRepository: EntityRepository[Int, SemesterEvent, SemesterState, Semester.Id, semester.Semester] = _

  @Before
  def setup(): Unit = {
    eventStore = newEventStore
    StudentRepository = new EntityRepository(Student, ec)(eventStore)
    SemesterRepository = new EntityRepository(Semester, ec)(eventStore)
  }

  @After
  def after(): Unit = {
    eventStore.ticker.close()
  }

  private def randomName(): String = (
    rand.nextBetween('A', 'Z' + 1) +: (1 to rand.nextBetween(2, 13)).map(_ => rand.nextBetween('a', 'z' + 1))).mkString

  private def addStudents(count: Int): Seq[Student.Id] = {
    val ids =
      for (_ <- 1 to count) yield {
        val name = randomName()
        val student = Student(RegisterStudent(name, EmailAddress(s"$name@school.edu")))
        student(AddStudentEmail(EmailAddress(s"$name@gmail.com")))
        StudentRepository.insert(new Student.Id(rand.nextInt), student)
      }
    Future.sequence(ids).await(60.seconds)
  }

  private def addSemesters(count: Int): Seq[Semester.Id] = {
    val ids =
      for (_ <- 1 to count) yield {
        val name = randomName() + " " + (100 + rand.nextBetween(1, 10))
        val cls = Semester(CreateClass(name))
        SemesterRepository.insert(new Semester.Id(rand.nextInt), cls)
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

  def newLookupServiceProcStore//(implicit updateCodec: UpdateCodec[StudentEmails, StudentEmailsUpdate])
      : StreamProcessStore[Int, StudentEmails, StudentEmailsUpdate] =
    InMemoryProcStore[StudentEmails, StudentEmailsUpdate](new TrieMap)

  def lookupService(procStore: StreamProcessStore[Int, StudentEmails, StudentEmailsUpdate])
    : LookupService = procStore match {
      case InMemoryProcStore(snapshots) =>
        new LookupService {
          def findStudent(email: EmailAddress): Future[Option[Student.Id]] = Future {
            val lowerCase = email.toLowerCase
            val results = snapshots.mapValues(_.content.emails).filter {
              case (_, emails) => emails contains lowerCase
            }
            if (results.size > 1) sys.error("Probably a randomization clash. Please run again.")
            results.headOption.map(_._1).map(new Student.Id(_))
          }
        }
      case _ => sys.error("This method should be overwritten when `def procStore[S]` is overwritten.")
    }

  private case class InMemoryProcStore[S, U](
    snapshots: collection.concurrent.Map[Int, delta.Snapshot[S]])(
      implicit
      protected val updateCodec: UpdateCodec[S, U])
  extends StreamProcessStore[Int, S, U] with BlockingCASWrites[Int, S, U, Unit] {

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
        assert(model == null)
        StudentEmails(Set(email))

      case StudentEmailAdded(email) => model.copy(model.emails + email)
      case StudentEmailRemoved(email) => model.copy(model.emails - email)

      case _ => model
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
      val evtHandler = tx.channel match {
        case Student.channel => onStudent(new Student.Id(tx.stream)) _
        case Semester.channel => onSemester(new Semester.Id(tx.stream)) _
      }
      tx.events.foreach(evtHandler)
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

    val eventStore = this.eventStore

    case class SemesterRev(id: Semester.Id, rev: Int) {
      def this(id: Int, rev: Int) = this(new Semester.Id(id), rev)
      override val toString = s"${id.int}:$rev"
    }
    sealed abstract class Model
    case class StudentModel(enrolled: Set[SemesterRev] = Set.empty) extends Model
    case class SemesterModel(students: Set[Student.Id] = Set.empty) extends Model

    type State = ConcurrentMapStore.State[Model]
    import ConcurrentMapStore.State

    class ModelBuilder(memMap: TrieMap[Int, State])
      extends MonotonicReplayProcessor[Int, SemesterEvent, Model, Model, Unit](
        10.seconds,
        ConcurrentMapStore(memMap, None)(_ => Future.none))
      with MonotonicJoinState[Int, SemesterEvent, Model, Model] {

      protected def prepareJoin(
          semesterId: Int, semesterRev: Int, tick: Long, md: Map[String, String])(
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

    val procStore = newLookupServiceProcStore
    val lookup = lookupService(procStore)
    object ServiceBuilder extends PersistentMonotonicConsumer[Int, StudentEvent, StudentEmails, StudentEmailsUpdate] {
      protected def processStore = procStore
      protected def replayMissingDelay = 2.seconds
      protected def replayMissingScheduler = Scheduler
      protected def replayPersistenceBatchSize: Int = 100
      protected def replayPersistenceContext: ExecutionContext = RandomDelayExecutionContext
      // RandomDelayExecutionContext
      protected def maxTickSkew: Int = 2
      protected def selector(es: EventSource): es.Selector = es.ChannelSelector(Student.channel)
      protected def onUpdate(id: Int, update: Update): Unit = ()
      protected def reportFailure(th: Throwable): Unit = {
        th.printStackTrace()
        fail(th.getMessage)
      }
      protected def process(tx: Transaction, currState: Option[StudentEmails]) =
        StudentEmailsProjector(tx, currState)
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
