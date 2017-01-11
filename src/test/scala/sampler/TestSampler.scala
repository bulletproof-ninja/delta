package sampler

import java.io.File
import java.sql.ResultSet
import scala.concurrent.{ Await, ExecutionContext }
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{ Failure, Random, Success, Try }
import org.junit.{ Before, Test }
import org.junit.AfterClass
import org.junit.Assert._
import sampler.aggr._
import scuff._
import scuff.ddd.Repository
import ulysses.EventStore
import ulysses.ddd.{ EntityRepository }
import ulysses.SysClockTicker
import ulysses.util.LocalPublishing
import scuff.ddd.DuplicateIdException
import scuff.concurrent.{
  StreamCallback,
  StreamPromise
}
import ulysses.EventSource
import scala.concurrent.Promise
import ulysses._
import scuff.reflect.Surgeon
import ulysses.util.TransientEventStore
import sampler.aggr.emp.EmpEvent
import sampler.aggr.emp.EmpEvent
import sampler.aggr.dept.DeptEvent
import ulysses.testing.RandomDelayExecutionContext

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
  def inserting {
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
      case Failure(th: DuplicateIdException) => // Expected
      case Failure(th) => fail(s"Should have thrown ${classOf[DuplicateIdException].getSimpleName}, not $th")
    }
  }

  @Test
  def updating {
    val id = new EmpId
    assertTrue(EmployeeRepo.exists(id).await.isEmpty)
    val emp = register(id, RegisterEmployee("John Doe", "555-55-5555", new MyDate(1988, 4, 1), 43000, "Janitor"))
    val insertRev = EmployeeRepo.insert(id, emp, metadata).await
    assertEquals(0, insertRev)
    @volatile var updateRev = -1
    var updatedRev = EmployeeRepo.update(id, 0, metadata) {
      case (emp, revision) =>
        updateRev = revision
        Future successful emp(UpdateSalary(45000))
    }.await
    assertEquals(0, updateRev)
    assertEquals(1, updatedRev)
    updatedRev = EmployeeRepo.update(id, 1, metadata) {
      case (emp, revision) =>
        updateRev = revision
        Future successful emp(UpdateSalary(45000))
    }.await
    assertEquals(1, updateRev)
    assertEquals(1, updatedRev)
  }

  private def register(id: EmpId, cmd: RegisterEmployee): Employee = {
    val emp = Employee(cmd)
    EmployeeRepo.insert(id, emp, metadata).await
    emp
  }
}
