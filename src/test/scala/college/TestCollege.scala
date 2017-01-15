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
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import delta.util.LocalPublishing
import delta.testing.RandomDelayExecutionContext

class TestCollege {

  implicit def sem2fut(s: Semester): Future[Semester] = Future successful s
  implicit def stu2fut(s: Student): Future[Student] = Future successful s

  lazy val eventStore: EventStore[Int, CollegeEvent, String] =
    new TransientEventStore[Int, CollegeEvent, String, Array[Byte]](
      RandomDelayExecutionContext) with LocalPublishing[Int, CollegeEvent, String] {
      def publishCtx = RandomDelayExecutionContext
    }

  implicit def ec = RandomDelayExecutionContext
  implicit lazy val ticker = LamportTicker(eventStore)

  type TXN = eventStore.TXN

  lazy val StudentRepository: student.Repository =
    new EntityRepository("Student", student.Student)(eventStore)

  lazy val SemesterRepository: semester.Repository =
    new EntityRepository("Semester", semester.Semester)(eventStore)

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
  private def addSemesters(approx: Int) = {
    val ids =
      for (_ <- 1 to approx) yield {
        val name = randomName() + " " + (100 + rand.nextInRange(1 to 9))
        val id = new SemesterId(rand.nextInt)
        val cls = Semester(CreateClass(name))
        SemesterRepository.insert(id, cls).map(_ => id)
      }
    Future.sequence(ids).await
  }

  @Test
  def `many-to-many relationship` {
    val Unknown = "<unknown>"
    val studentIds = addStudents(1500)
    val semesterIds = addSemesters(90).toIndexedSeq
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
            semester
        }
      }
    }
    studentIds.filter(_ => rand.nextFloat >= 0.5f).foreach { studentId =>
      if (rand.nextBoolean) {
        SemesterRepository.update(randomSemester) {
          case (semester, _) =>
            semester(EnrollStudent(studentId))
            semester
        }
      }
      StudentRepository.update(studentId) {
        case (student, _) =>
          student(ChangeStudentName(randomName))
          student
      }
      if (rand.nextBoolean) {
        SemesterRepository.update(randomSemester) {
          case (semester, _) =>
            semester(EnrollStudent(studentId))
            semester
        }
      }
    }
    val enrollmentQuery = eventStore.query(eventStore.EventSelector(Map(
      "Student" -> Set(classOf[StudentChangedName], classOf[StudentRegistered]),
      "Semester" -> Set(classOf[StudentEnrolled])))) _

    val allStudents = new TrieMap[StudentId, (Set[SemesterId], String)].withDefaultValue(Set.empty -> Unknown)
    val readModel = new TrieMap[SemesterId, Map[StudentId, String]].withDefaultValue(Map.empty)
    val done = StreamPromise.foreach(enrollmentQuery) { txn =>
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
}
