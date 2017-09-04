package sampler

import scala.util.{ Failure, Success, Try }

import org.junit.Assert._
import org.junit.Test

import delta.{ EventStore, LamportTicker }
import delta.ddd.{ DuplicateIdException, EntityRepository, Repository, Revision }
import delta.testing.RandomDelayExecutionContext
import delta.util.{ LocalPublishing, TransientEventStore }
import sampler.aggr.{ Department, DomainEvent, Employee, RegisterEmployee, UpdateSalary }

class TestSampler {

  def metadata = Map("timestamp" -> new scuff.Timestamp().toString)

  lazy val es: EventStore[Int, DomainEvent, Aggr.Value] =
    new TransientEventStore[Int, DomainEvent, Aggr.Value, JSON](
      RandomDelayExecutionContext) with LocalPublishing[Int, DomainEvent, Aggr.Value] {
      def publishCtx = RandomDelayExecutionContext
    }

  implicit def ec = RandomDelayExecutionContext
  implicit lazy val ticker = LamportTicker(es)

  lazy val EmployeeRepo: Repository[EmpId, Employee] =
    new EntityRepository(Aggr.Empl, Employee.Def)(es)
  lazy val DepartmentRepo: Repository[DeptId, Department] =
    new EntityRepository(Aggr.Dept, Department.Def)(es)

  @Test
  def inserting() {
    val id = new EmpId
    assertFalse(EmployeeRepo.exists(id).await.isDefined)
    val register = RegisterEmployee("John Doe", "555-55-5555", new MyDate(1988, 4, 1), 43000, "Janitor")
    val emp = Employee(register)
    val insertRev = EmployeeRepo.insert(id, emp, metadata).await
    assertEquals(0, insertRev)
    Try(EmployeeRepo.insert(id, emp, metadata).await) match {
      case Success(revision) =>
        // Allow idempotent inserts
        assertEquals(0, revision)
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
    val insertRev = EmployeeRepo.insert(id, emp, metadata).await
    assertEquals(0, insertRev)
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
    @volatile var updateRev = -1
    var updatedRev = EmployeeRepo.update(id, Revision(0), metadata) {
      case (emp, revision) =>
        updateRev = revision
        emp(UpdateSalary(45000))
    }.await
    assertEquals(0, updateRev)
    assertEquals(1, updatedRev)
    updatedRev = EmployeeRepo.update(id, Revision(0), metadata) {
      case (emp, revision) =>
        updateRev = revision
        emp(UpdateSalary(45000))
    }.await
    assertEquals(1, updateRev)
    assertEquals(1, updatedRev)
    try {
      EmployeeRepo.update(id, Revision.Exactly(0), metadata) {
        case (emp, revision) =>
          updateRev = revision
          emp(UpdateSalary(66000))
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
