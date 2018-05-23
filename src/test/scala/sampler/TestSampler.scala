package sampler

import scala.util.{ Failure, Success, Try }

import org.junit.Assert._
import org.junit.Test

import delta.{ EventStore, LamportTicker, Publishing }
import delta.ddd.{ DuplicateIdException, EntityRepository, Revision }
import delta.testing.RandomDelayExecutionContext
import delta.util.TransientEventStore
import sampler.aggr.{ Department, DomainEvent, Employee, RegisterEmployee, UpdateSalary }
import delta.util.LocalPublisher

class TestSampler {

  def metadata = Map("timestamp" -> new scuff.Timestamp().toString)

  lazy val es: EventStore[Int, DomainEvent] =
    new TransientEventStore[Int, DomainEvent, JSON](
      RandomDelayExecutionContext) with Publishing[Int, DomainEvent] {
      val publisher = new LocalPublisher[Int, DomainEvent](RandomDelayExecutionContext)
    }

  implicit def ec = RandomDelayExecutionContext
  implicit lazy val ticker = LamportTicker(es)

  lazy val EmployeeRepo = new EntityRepository(Employee.Def)(es)
  lazy val DepartmentRepo = new EntityRepository(Department.Def)(es)

  @Test
  def inserting() {
    val id = new EmpId
    assertFalse(EmployeeRepo.exists(id).await.isDefined)
    val register = RegisterEmployee("John Doe", "555-55-5555", new MyDate(1988, 4, 1), 43000, "Janitor")
    val emp = Employee(register)
    val insertId = EmployeeRepo.insert(id, emp, metadata).await
    assertEquals(id, insertId)
    Try(EmployeeRepo.insert(id, emp, metadata).await) match {
      case Success(idAgain) =>
        // Allow idempotent inserts
        assertEquals(id, idAgain)
      case Failure(th) =>
        fail(s"Should succeed, but didn't: $th")
    }
    emp.apply(UpdateSalary(40000))
    Try(EmployeeRepo.insert(id, emp, metadata).await) match {
      case Success(revision) => fail(s"Should fail, but inserted revision $revision")
      case Failure(DuplicateIdException(dupe)) => assertEquals(id, dupe)
      case Failure(th) => fail(s"Should have thrown ${classOf[DuplicateIdException].getSimpleName}, not $th")
    }
  }

  @Test
  def updating() {
    val id = new EmpId
    assertTrue(EmployeeRepo.exists(id).await.isEmpty)
    val emp = register(id, RegisterEmployee("John Doe", "555-55-5555", new MyDate(1988, 4, 1), 43000, "Janitor"))
    val insertId = EmployeeRepo.insert(id, emp, metadata).await
    assertEquals(id, insertId)
    try {
      EmployeeRepo.update(id, Revision(3), metadata) {
        case (emp, _) =>
          emp(UpdateSalary(45000))
      }.await
      fail("Should throw a Revision.MismatchException")
    } catch {
      case Revision.MismatchException(expected, actual) =>
        assertEquals(3, expected)
        assertEquals(0, actual)
    }
    var revs = EmployeeRepo.update(id, Revision(0), metadata) {
      case (emp, revision) =>
        emp(UpdateSalary(45000))
        revision
    }.await
    assertEquals(0, revs._1)
    assertEquals(1, revs._2)
    revs = EmployeeRepo.update(id, Revision(0), metadata) {
      case (emp, revision) =>
        emp(UpdateSalary(45000))
        revision
    }.await
    assertEquals(1, revs._1)
    assertEquals(1, revs._2)
    try {
      EmployeeRepo.update(id, Revision.Exactly(0), metadata) {
        case (emp, revision) =>
          emp(UpdateSalary(66000))
          revision
      }.await
      fail("Should throw a Revision.Mismatch")
    } catch {
      case Revision.MismatchException(expected, actual) =>
        assertEquals(0, expected)
        assertEquals(1, actual)
    }
  }

  private def register(id: EmpId, cmd: RegisterEmployee): Employee = {
    val emp = Employee(cmd)
    EmployeeRepo.insert(id, emp, metadata).await
    emp
  }
}
