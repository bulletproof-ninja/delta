package college

import java.util.concurrent.ScheduledExecutorService

import delta._
import delta.util._
import delta.write._
import delta.testing._
import delta.process._, LiveProcessConfig._
import delta.validation.ConsistencyValidation
import college.validation.EmailValidationProcess.State
import delta.read.SnapshotReaderSupport
import delta.read.ReadModel
import delta.read.MessageHubSupport

import college._, student.Student, semester.Semester

import scuff._
import scuff.concurrent.{ Threads, ScuffFutureObject }

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Random => rand }
import scala.util.Success
import scala.util.Failure

import college.validation._

// FIXME: Something's not right with the test case. Code coverage is incomplete.
class TestCollege
extends BaseTest {

  implicit def newCollegeMD: CollegeMD = new CollegeMD
  // implicit def newCollegeMD(unit: Unit)(implicit md: CollegeMD): Future[CollegeMD] = Future successful md

  type Transaction = delta.Transaction[Int, CollegeEvent]
  val Scheduler = Threads.newScheduledThreadPool(8, Threads.factory(Threads.SystemThreadGroup), _.printStackTrace)

  val replayConfig = ReplayProcessConfig(2.minutes, 100)
  val liveConfig = LiveProcessConfig(ImmediateReplay)

  private class TransientCollegeEventStore
  extends TransientEventStore[Int, CollegeEvent, Array[Byte]](ec, CollegeEventFormat)
  with ConsistencyValidation[Int, CollegeEvent] {

    val ticker = LamportTicker(new LamportClock(0))
    def validationContext(stream: Int) = ec

    // val txChannels = Set(student.Channel, semester.Channel)
    // def toTopic(ch: Channel) = MessageTransport.Topic(ch.toString)
    // val txTransport = new LocalTransport[Transaction](ec)
    // val txTransportCodec = Codec.noop[Transaction]

  }

  def newEventStore[MT](
      allChannels: Set[Channel],
      txTransport: MessageTransport[MT])(
      implicit
      encode: Transaction => MT,
      decode: MT => Transaction)
      : college.CollegeEventStore =
    new TransientCollegeEventStore().asInstanceOf[CollegeEventStore]

  def newEmailIndexUpdateHub(): UpdateHub[Int, Unit] = {
    type Msg = (Int, delta.process.Update[Unit])
    val topic = MessageTransport.Topic("email-index-update")
    val transport = new delta.util.LocalTransport[Msg](ec)
    MessageHub(transport, topic)
  }

  protected var eventStore: CollegeEventStore = null

  implicit val mdFunc = () => newCollegeMD

  protected var StudentRepository: StudentRepo = null
  protected var SemesterRepository: SemesterRepo = null
  protected var EmailIndexStore: EmailValidationProcessStore = null
  protected var EmailIndexUpdates: UpdateHub[Int, Unit] = null

  override def beforeEach(): Unit = {

    this.eventStore = {
      val transport = new delta.util.LocalTransport[Transaction](ec)
      val channels = college.AllEntities.map(_.channel)
      newEventStore(channels, transport)
    }

    EmailIndexStore = newEmailValidationProcessStore()
    EmailIndexUpdates = newEmailIndexUpdateHub()
    this.StudentRepository = new CollegeRepo(student.Entity)(eventStore)
    this.SemesterRepository = new CollegeRepo(semester.Entity)(eventStore)

    val emailValidation =
      new EmailValidationProcess(
        EmailIndexStore)(
        StudentRepository, EmailIndexStore, EmailIndexUpdates)

    val replayProcess = emailValidation.validate(eventStore, replayConfig)
    val completion = replayProcess.finished.await
    assert(completion.incompleteStreams.isEmpty)
    assert(0 === replayProcess.activeTransactions)
    println(s"Total transactions: ${replayProcess.totalTransactions}")
    eventStore.activate(emailValidation, liveConfig)

  }

  override def afterEach(): Unit = {
    eventStore.ticker.close()
  }

  private def addStudents(count: Int): Seq[Student.Id] = {
    val ids =
      for (_ <- 1 to count) yield {
        val name = randomName
        val st = Student { student.RegisterStudent(name, EmailAddress(s"$name@school.edu")) }
        st apply student.AddStudentEmail(EmailAddress(s"$name@gmail.com"))
        StudentRepository.insert(new Student.Id, st)
      }
    Future.sequence(ids).await
  }

  private def addSemesters(count: Int): Seq[Semester.Id] = {
    val ids =
      for (_ <- 1 to count) yield {
        val name = randomName + " " + (100 + rand.nextBetween(1, 10))
        val cls = Semester { semester.CreateClass(name) }
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
          _._1 apply semester.EnrollStudent(studentId)
        }
      }
    }
    (Future sequence futureEnrollments).await

    val futureUpdates = studentIds.filter(_ => rand.nextFloat() >= 0.5f).flatMap { studentId =>
      val futureEnrollment1 =
        if (rand.nextBoolean()) {
          SemesterRepository.update(randomSemester) {
            _._1 apply semester.EnrollStudent(studentId)
          }
        } else Future.unit
      val futureNameChange = StudentRepository.update(studentId) {
        _._1 apply student.ChangeStudentName(randomName)
      }
      val futureEnrollment2 = if (rand.nextBoolean()) {
        SemesterRepository.update(randomSemester) {
          _._1 apply semester.EnrollStudent(studentId)
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

  val UnknownName = "[unknown]"

  // test("many-to-many relationship") {

  //   val eventStore = this.eventStore
  //   type Transaction = eventStore.Transaction

  //   case class StudentM(
  //     name: String = UnknownName,
  //     emails: Set[EmailAddress] = Set.empty,
  //     enrolledClasses: Map[Semester.Id, String] = Map.empty)
  //   case class SemesterM(
  //     className: String = UnknownName,
  //     students: Map[Student.Id, String] = Map.empty)

  //   val AnonStudent = StudentM()
  //   val AnonSemester = SemesterM()

  //   populateRepos(100, 15)
  //   val enrollmentQuery = eventStore.query(eventStore.ChannelSelector(Student.channel, Semester.channel)) _

  //   val allStudents = new TrieMap[Student.Id, StudentM]
  //   val allSemesters = new TrieMap[Semester.Id, SemesterM]
  //   val done = FutureReduction.foreach(enrollmentQuery) { tx: Transaction =>
  //       def onSemester(semesterId: Semester.Id)(evt: semester.SemesterEvent): Unit = {
  //         import semester._
  //         evt match {
  //           case ClassCreated(className) =>
  //             allSemesters.putIfAbsent(semesterId, SemesterM(className)) match {
  //               case None => // Ok
  //               case Some(existing) =>
  //                 if (!allSemesters.replace(semesterId, existing, existing.copy(className = className))) {
  //                   onSemester(semesterId)(evt)
  //                 }
  //             }
  //           case StudentEnrolled(studentId) =>
  //             val sem = allSemesters.getOrElse(semesterId, AnonSemester)
  //             val stu = allStudents.getOrElse(studentId, AnonStudent)
  //             allStudents.replace(studentId, stu, stu.copy())
  //             readModel.update(semesterId, semesterStudents.updated(studentId, studentModel))
  //           case _ => ???
  //         }
  //       }
  //       def studentNameChange(studentName: String, studentId: Student.Id): Unit = {
  //         val (studentSemesters, oldStudent) = allStudents(studentId)
  //         val studentModel = oldStudent.copy(name = studentName)
  //         allStudents.update(studentId, (studentSemesters, studentModel))
  //         studentSemesters.foreach { semesterId =>
  //           val semesterStudents = readModel(semesterId)
  //           readModel.update(semesterId, semesterStudents.updated(studentId, studentModel))
  //         }
  //       }
  //       def studentEmailChange(email: Either[EmailAddress, EmailAddress], studentId: Student.Id): Unit = {
  //         val (studentSemesters, oldStudent) = allStudents(studentId)
  //         val studentModel = email match {
  //           case Left(remove) => oldStudent.copy(emails = oldStudent.emails - remove)
  //           case Right(add) => oldStudent.copy(emails = oldStudent.emails + add)
  //         }
  //         allStudents.update(studentId, (studentSemesters, studentModel))
  //         studentSemesters.foreach { semesterId =>
  //           val semesterStudents = readModel(semesterId)
  //           readModel.update(semesterId, semesterStudents.updated(studentId, studentModel))
  //         }
  //       }
  //       def onStudent(studentId: Student.Id)(evt: student.StudentEvent) = {
  //         import student._
  //         evt match {
  //           case StudentRegistered(name) =>
  //             studentNameChange(name, studentId)
  //           case StudentChangedName(newName) =>
  //             studentNameChange(newName, studentId)
  //           case StudentEmailAdded(newEmail) =>
  //             studentEmailChange(Right(EmailAddress(newEmail)), studentId)
  //           case StudentEmailRemoved(removeEmail) =>
  //             studentEmailChange(Left(EmailAddress(removeEmail)), studentId)
  //           case _ => ???
  //         }
  //       }
  //     val evtHandler = tx.channel match {
  //       case Student.channel =>
  //         tx.events
  //           .collect { case evt: student.StudentEvent => evt }
  //           .foreach { onStudent(new Student.Id(tx.stream)) _ }
  //       case Semester.channel =>
  //         tx.events
  //           .collect { case evt: semester.SemesterEvent => evt }
  //           .foreach { onSemester(new Semester.Id(tx.stream)) _ }
  //       case _ => ???
  //     }
  //   }
  //   done.await
  //   allStudents.foreach {
  //     case (id, (_, name)) =>
  //       assert(name !== UnknownName, s"Student $id name was not updated")
  //   }
  //   readModel.foreach {
  //     case (semesterId, students) =>
  //       students.foreach {
  //         case (studentId, name) =>
  //           val (studentSemesters, studentName) = allStudents(studentId)
  //           assert(studentName === name)
  //           assert(studentSemesters.contains(semesterId))
  //       }
  //   }
  // }

  test("with join state") {

    val eventStore = this.eventStore

    // case class SemesterRev(id: Semester.Id, rev: Revision) {
    //   def this(id: Int, rev: Revision) = this(new Semester.Id(id), rev)
    //   override val toString = s"${id.int}:$rev"
    // }
    sealed abstract class Model
    case class StudentM(name: String = UnknownName, enrolled: Map[Semester.Id, Revision] = Map.empty) extends Model
    case class SemesterM(className: String = UnknownName, enrolledStudentNames: Map[Student.Id, String] = Map.empty) extends Model

    type ReplayState = delta.process.ReplayState[Model]

    class ModelReplayBuilder(memMap: TrieMap[Int, ReplayState])
    extends MonotonicReplayProcessor[Int, CollegeEvent, Model, Model](
      ReplayProcessConfig(60.seconds, 100)) {
      join: MonotonicJoinProcessor[Int, CollegeEvent, Model, Model] =>
      protected def executionContext = ec

      protected val processStore: StreamProcessStore[Int, Model, Model] =
        ConcurrentMapStore(memMap, "semester", None)(_ => Future.none)

      val replayThreads = scuff.concurrent.PartitionedExecutionContext(2, _.printStackTrace(System.err))
      def asyncResult(status: ReplayCompletion[Int]): Future[ReplayCompletion[Int]] = {
        println("Shutting down stream partitions...")
        replayThreads.shutdown()
          .map(_ => status)
          .andThen {
            case Success(_) => println("Stream partitions shut down successfully.")
            case Failure(th) => th.printStackTrace(System.err)
          }
      }
      protected def processContext(id: Int) = replayThreads.singleThread(id)
      override protected def onUpdate(id: Int, update: Update[Model]) = update match {
        case Update(Some(StudentM(_, enrolled)), _, _) =>
          println(s"Student $id is currently enrolled in: $enrolled")
        case Update(Some(SemesterM(name, students)), rev, _) =>
          println(s"Semester $id:$rev ($name) currently has enrolled: $students")
        case _ => // Ignore
      }

      protected def EnrichmentEvaluator(
          refTx: Transaction, refState: Model) = refState match {
        case student: StudentM => SemesterEnrichmentFromStudent(new Student.Id(refTx.stream), student)
        case _: SemesterM => StudentEnrichmentFromSemester(new Semester.Id(refTx.stream), refTx.revision)
        case _ => PartialFunction.empty
      }

      private def SemesterEnrichmentFromStudent(studentId: Student.Id, studentM: StudentM): EnrichmentEvaluator = {
        case student.StudentChangedName(newName) => Enrichment(studentM.enrolled.keySet, new SemesterM) { semester =>
          val updatedStudents = semester.enrolledStudentNames.updated(studentId, newName)
          semester.copy(enrolledStudentNames = updatedStudents)
        }
      }
      private def StudentEnrichmentFromSemester(semesterId: Semester.Id, semesterRev: Revision): EnrichmentEvaluator = {
        case semester.StudentEnrolled(studentId) => Enrichment(studentId, new StudentM) { student =>
          val enrolled = student.enrolled.updated(semesterId, semesterRev)
          student.copy(enrolled = enrolled)
        }
        case semester.StudentCancelled(studentId) => Enrichment(studentId, new StudentM) { student =>
          val unenrolled = student.enrolled - semesterId
          student.copy(enrolled = unenrolled)
        }
      }

      protected def process(tx: Transaction, currState: Option[Model]) =
        tx.channel match {
          case Student.channel =>
            val student = currState.collectAs[StudentM] getOrElse StudentM()
            tx.events.foldLeft(student) {
              case _ => student
            }
          case Semester.channel =>
            val sem = currState.collectAs[SemesterM] getOrElse SemesterM()
            tx.events.foldLeft(sem) {
              case (sem, semester.StudentEnrolled(studentId)) =>
                val students = sem.enrolledStudentNames.updated(studentId, UnknownName)
                sem.copy(enrolledStudentNames = students)
              case (sem, semester.StudentCancelled(studentId)) =>
                val students = sem.enrolledStudentNames - studentId
                sem.copy(enrolledStudentNames = students)
              case _ => sem
            }
          case ch =>
            sys.error(s"Unhandled entity: $ch")
        }

    }

    val (_, semesterIds) = populateRepos(100, 20)
    val currSemesterRevs: Map[Semester.Id, Int] =
      (Future sequence semesterIds.map(id => eventStore.currRevision(id.int).map(rev => id -> rev)))
      .await
      .map {
        case (id, revOpt) => id -> revOpt.getOrElse { fail(s"Semester $id does not exist"); -1 }
      }.toMap
    val inMemoryMap = new TrieMap[Int, ReplayState]
    val queryFuture: Future[ReplayCompletion[Int]] = eventStore.query(eventStore.Selector(Semester.channel)) {
      new ModelReplayBuilder(inMemoryMap)
      with MonotonicJoinProcessor[Int, CollegeEvent, Model, Model]
    }
    val replayCompletion = queryFuture.await
    assert(replayCompletion.incompleteStreams.isEmpty)
    inMemoryMap.foreach {
      case (semesterIntId, ReplayState(Snapshot(SemesterM(_, students), inMemSemesterRev, _), _)) =>
        val semesterId = new Semester.Id(semesterIntId)
        val currSemesterRev = currSemesterRevs.get(semesterId) match {
          case Some(rev) => rev
          case None =>
            if (semesterIds contains semesterId) {
              eventStore.currRevision(semesterId.int).await getOrElse sys.error(s"Semester $semesterId does not exist in event store, which should be impossible")
            } else fail(s"Semester $semesterId exists in inMemoryMap, but not in event store. That is basically impossible")
        }
        assert(currSemesterRev === inMemSemesterRev, s"Semester $semesterId revision $inMemSemesterRev failed")
        val studentSemesters: Iterable[Set[Semester.Id]] =
          students.keys
            .flatMap(id => inMemoryMap.get(id.int))
            .collect {
              case ReplayState(Snapshot(StudentM(_, semesters), _, _), _) =>
                semesters.keySet
            }
        studentSemesters.foreach { semesters =>
          assert(semesters contains semesterId)
        }
      case (studentIntId, ReplayState(Snapshot(StudentM(_, semesters), rev, _), _)) =>
        assert(-1 === rev)
        val studentId = new Student.Id(studentIntId)
        val semesterStudents: Iterable[Set[Student.Id]] =
          semesters
            .map(_ => inMemoryMap(studentId.int))
            .collect {
              case ReplayState(Snapshot(SemesterM(_, students), _, _), _) =>
                students.keySet
            }
        semesterStudents.foreach { students =>
          assert(students contains studentId)
        }
    }
  }

  test("lookup by email") {

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
        def name = "unit"
        protected type StreamId = Int
        protected def StreamId(id: Student.Id): StreamId = id.int
        protected val snapshotReader = EmailIndexStore.asSnapshotReader[Int, Unit]

        private[this] val SomeUnit = Some(())
        protected def updateState(
            id: Student.Id, prevState: Option[Unit], update: Unit) = SomeUnit
        protected val defaultReadTimeout: FiniteDuration = 60.seconds
        protected def scheduler: ScheduledExecutorService = Scheduler
        protected def updateHub: MessageHub[StreamId, Update] = EmailIndexUpdates
    }

    val emailA = EmailAddress("A@school.edu")
    val studentA = Student { student.RegisterStudent("A", emailA) }
    val idA = StudentRepository.insert(new Student.Id, studentA).await

    val emailB = EmailAddress("B@school.edu")
    val studentB = Student { student.RegisterStudent("B", emailB) }
    val idB = StudentRepository.insert(new Student.Id, studentB).await

    val dupes = EmailIndexStore.findDuplicates().await
    assert(dupes.isEmpty, s"Duplicates found: $dupes")

    emailIndexModel.readCustom(idA)(minRevision(0)).await
    assert(idA === EmailIndexStore.lookup(emailA).await.get)

    emailIndexModel.read(idB, minRevision = 0).await
    assert(idB === EmailIndexStore.lookup(emailB).await.get)

    val emailA2 = EmailAddress("a@SCHOOL.EDU")
    val emailB2 = EmailAddress("b@SCHOOL.EDU")
    assert(idA === EmailIndexStore.lookup(emailA2).await.get)
    assert(idB === EmailIndexStore.lookup(emailB2).await.get)

    StudentRepository.update(idA) {
      case (stu, 0) =>
        stu apply student.AddStudentEmail(emailB2)
      case (_, rev) =>
        sys.error(s"Unexpected revision: $rev")
    }.await
    assert(idB === EmailIndexStore.lookup(emailB2).await.get)

    emailIndexModel.readCustom(idA)(minRevision(1)).await
    assert(idB === EmailIndexStore.lookup(emailB2).await.get)

    val snapshot = emailIndexModel.read(idA, minRevision = 2).await
    assert(2 === snapshot.revision)
    assert(1 === EmailIndexStore.lookupAll(emailB2).await.size)

  }
}
