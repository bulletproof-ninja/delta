package sampler

import scala.util.{ Failure, Success, Try }

import org.junit.Assert._
import org.junit.Test

import delta.{ EventStore, LamportTicker, MessageTransportPublishing }
import delta.write.{ DuplicateIdException, EntityRepository }
import delta.testing.RandomDelayExecutionContext
import delta.util.TransientEventStore
import sampler.aggr.{ Department, DomainEvent, Employee, RegisterEmployee, UpdateSalary }
import delta.util.LocalTransport
import scuff.Codec
import delta.write.Metadata

class TestSampler {

  implicit def metadata = Metadata("timestamp" -> new scuff.Timestamp().toString)

  protected def initTicker(es: EventStore[Int, DomainEvent]) = LamportTicker(es)

  lazy val es: EventStore[Int, DomainEvent] =
    new TransientEventStore[Int, DomainEvent, JSON](
         RandomDelayExecutionContext, JsonDomainEventFormat)(initTicker)
         with MessageTransportPublishing[Int, DomainEvent] {
      def toTopic(ch: Channel) = Topic(s"transactions/$ch")
      def toTopic(tx: Transaction): Topic = toTopic(tx.channel)
      val txTransport = new LocalTransport[Transaction](toTopic, RandomDelayExecutionContext)
      val txChannels = Set(Employee.Def.channel, Department.Def.channel)
      val txCodec = Codec.noop[Transaction]
    }

  implicit def ec = RandomDelayExecutionContext

  lazy val EmployeeRepo = new EntityRepository(Employee.Def, ec)(es)
  lazy val DepartmentRepo = new EntityRepository(Department.Def, ec)(es)

  @Test
  def inserting(): Unit = {
    val id = new EmpId
    assertFalse(EmployeeRepo.exists(id).await.isDefined)
    val register = RegisterEmployee("John Doe", "555-55-5555", new MyDate(1988, 4, 1), 43000, "Janitor")
    val emp = Employee(register)
    val insertId = EmployeeRepo.insert(id, emp).await
    assertEquals(id, insertId)
    Try(EmployeeRepo.insert(id, emp).await) match {
      case Success(idAgain) =>
        // Allow idempotent inserts
        assertEquals(id, idAgain)
      case Failure(th) =>
        fail(s"Should succeed, but didn't: $th")
    }
    emp.apply(UpdateSalary(40000))
    Try(EmployeeRepo.insert(id, emp).await) match {
      case Success(revision) => fail(s"Should fail, but inserted revision $revision")
      case Failure(dupe: DuplicateIdException) => assertEquals(id, dupe.id)
      case Failure(th) => fail(s"Should have thrown ${classOf[DuplicateIdException].getSimpleName}, not $th")
    }
  }

  @Test
  def updating(): Unit = {
    val id = new EmpId
    assertTrue(EmployeeRepo.exists(id).await.isEmpty)
    val emp = register(id, RegisterEmployee("John Doe", "555-55-5555", new MyDate(1988, 4, 1), 43000, "Janitor"))
    val insertId = EmployeeRepo.insert(id, emp).await
    assertEquals(id, insertId)
    try {
      EmployeeRepo.update(id) {
        case (emp, 3) =>
          emp(UpdateSalary(45000))
        case (_, rev) =>
          throw new IllegalStateException(rev.toString)
      }.await
      fail("Should throw a Revision.MismatchException")
    } catch {
      case e: IllegalStateException =>
        assertEquals("0", e.getMessage)
    }
    var revs = EmployeeRepo.update(id, Some(0)) {
      case (emp, revision) =>
        emp(UpdateSalary(45000))
        revision
    }.await
    assertEquals(0, revs._1)
    assertEquals(1, revs._2)
    revs = EmployeeRepo.update(id, Some(0)) {
      case (emp, revision) =>
        emp(UpdateSalary(45000))
        revision
    }.await
    assertEquals(1, revs._1)
    assertEquals(1, revs._2)
    try {
      EmployeeRepo.update(id) {
        case (emp, 0) =>
          emp(UpdateSalary(66000))
        case (_, rev) => throw new IllegalStateException(rev.toString)
      }.await
      fail("Should throw a Match exception")
    } catch {
      case e: IllegalStateException =>
        assertEquals("1", e.getMessage)
    }
  }

  private def register(id: EmpId, cmd: RegisterEmployee): Employee = {
    val emp = Employee(cmd)
    EmployeeRepo.insert(id, emp).await
    emp
  }
}
