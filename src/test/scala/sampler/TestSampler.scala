package sampler

import delta._
import delta.testing._
import delta.util._
import delta.write._

import sampler.aggr._

import scuff.LamportClock
import scala.concurrent.Future
import scala.util.control.NonFatal

class TestSampler
extends BaseTest {
  import delta.testing.F

  implicit def metadata = Metadata("timestamp" -> new scuff.Timestamp().toString)

  lazy val es: EventStore[Int, DomainEvent] =
    new TransientEventStore[Int, DomainEvent, JSON](
      ec, JsonDomainEventFormat) {
      val ticker: Ticker = LamportTicker(new LamportClock(0))
    }

  lazy val EmployeeRepo = new EntityRepository(Employee.Def)(es, ec)
  lazy val DepartmentRepo = new EntityRepository(Department.Def)(es, ec)

  test("inserting") {
    val id = new EmpId
    assert(EmployeeRepo.exists(id).await.isEmpty)
    val register = RegisterEmployee("John Doe", "555-55-5555", new MyDate(1988, 4, 1), 43000, "Janitor")
    val emp = Employee(register)
    val insertId = EmployeeRepo.insert(id, emp).await
    assert(id === insertId)
    val idAgain = EmployeeRepo.insert(id, emp).await
    // Allow idempotent inserts
    assert(id === idAgain)

    emp.apply(UpdateSalary(40000))
    // No longer idempotent, thus fail
    try {
      EmployeeRepo.insert(id, emp).await
      fail(s"Should fail, but inserted revision 0 again for $id")
    } catch {
      case dupe: DuplicateIdException =>
        assert(id === dupe.id)
      case NonFatal(th) =>
        fail(s"Should have thrown ${classOf[DuplicateIdException].getSimpleName}, not: $th")
    }
  }

  test("updating") {
    val id = new EmpId
    assert(EmployeeRepo.exists(id).await.isEmpty)
    val emp = register(id, RegisterEmployee("John Doe", "555-55-5555", new MyDate(1988, 4, 1), 43000, "Janitor"))
    implicit def toMetadata(emp: Employee): Future[Metadata] = Future successful metadata
    val insertId = EmployeeRepo.insert(id, emp).await
    assert(id === insertId)
    try {
      EmployeeRepo.update(id) {
        case (emp, 3) =>
          emp(UpdateSalary(45000))
          metadata
        case (_, rev) =>
          throw new IllegalStateException(rev.toString)
      }.await
      fail("Should throw a Revision.MismatchException")
    } catch {
      case e: IllegalStateException =>
        assert("0" === e.getMessage)
    }
    @volatile var fromRev = -1
    var toRev = EmployeeRepo.update(id, Some(0)) {
      case (emp, revision) =>
        fromRev = revision
        emp(UpdateSalary(45000))
    }.await
    assert(0 === fromRev)
    assert(1 === toRev)
    toRev = EmployeeRepo.update(id, Some(0)) {
      case (emp, revision) =>
        fromRev = revision
        emp(UpdateSalary(45000))
    }.await
    assert(1 === fromRev)
    assert(1 === toRev)
    try {
      EmployeeRepo.update(id) {
        case (emp, 0) =>
          emp(UpdateSalary(66000))
          metadata
        case (_, rev) => throw new IllegalStateException(rev.toString)
      }.await
      fail("Should throw a Match exception")
    } catch {
      case e: IllegalStateException =>
        assert("1" === e.getMessage)
    }
  }

  private def register(id: EmpId, cmd: RegisterEmployee): Employee = {
    val emp = Employee(cmd)
    EmployeeRepo.insert(id, emp).await
    emp
  }
}
