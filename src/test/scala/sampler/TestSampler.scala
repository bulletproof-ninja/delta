package sampler

import scala.util.{ Failure, Success, Try }

import org.junit.Assert._
import org.junit.Test

import delta.{ EventStore, LamportTicker, MessageHubPublishing }
import delta.ddd.{ DuplicateIdException, EntityRepository }
import delta.testing.RandomDelayExecutionContext
import delta.util.TransientEventStore
import sampler.aggr.{ Department, DomainEvent, Employee, RegisterEmployee, UpdateSalary }
import delta.util.LocalHub
import scuff.Codec

class TestSampler {

  def metadata = Map("timestamp" -> new scuff.Timestamp().toString)

  lazy val es: EventStore[Int, DomainEvent] =
    new TransientEventStore[Int, DomainEvent, JSON](
      RandomDelayExecutionContext, JsonDomainEventFormat) with MessageHubPublishing[Int, DomainEvent] {
      def toTopic(ch: Channel) = Topic(s"transactions/$ch")
      def toTopic(txn: TXN): Topic = toTopic(txn.channel)
      val txnHub = new LocalHub[TXN](toTopic, RandomDelayExecutionContext)
      val txnChannels = Set(Employee.Def.channel, Department.Def.channel)
      val txnCodec = Codec.noop
    }

  implicit def ec = RandomDelayExecutionContext
  lazy val ticker = LamportTicker(es)

  lazy val EmployeeRepo = new EntityRepository(Employee.Def, ec)(es, ticker)
  lazy val DepartmentRepo = new EntityRepository(Department.Def, ec)(es, ticker)

  @Test
  def inserting(): Unit = {
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
  def updating(): Unit = {
    val id = new EmpId
    assertTrue(EmployeeRepo.exists(id).await.isEmpty)
    val emp = register(id, RegisterEmployee("John Doe", "555-55-5555", new MyDate(1988, 4, 1), 43000, "Janitor"))
    val insertId = EmployeeRepo.insert(id, emp, metadata).await
    assertEquals(id, insertId)
    try {
      EmployeeRepo.update(id, metadata) {
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
    var revs = EmployeeRepo.update(id, Some(0), metadata) {
      case (emp, revision) =>
        emp(UpdateSalary(45000))
        revision
    }.await
    assertEquals(0, revs._1)
    assertEquals(1, revs._2)
    revs = EmployeeRepo.update(id, Some(0), metadata) {
      case (emp, revision) =>
        emp(UpdateSalary(45000))
        revision
    }.await
    assertEquals(1, revs._1)
    assertEquals(1, revs._2)
    try {
      EmployeeRepo.update(id, metadata) {
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
    EmployeeRepo.insert(id, emp, metadata).await
    emp
  }
}
