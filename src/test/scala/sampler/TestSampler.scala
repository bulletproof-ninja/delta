package sampler

import java.io.File
import java.sql.ResultSet
import java.util.UUID

import scala.concurrent.{ Await, ExecutionContext }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{ Failure, Random, Success, Try }

import org.h2.jdbcx.JdbcDataSource
import org.junit.{ Before, Test }
import org.junit.AfterClass
import org.junit.Assert._

import sampler.aggr._
import scuff._
import scuff.ddd.Repository
import ulysses.EventStore
import ulysses.ddd.{ EntityRepository, LamportClock }
import ulysses.jdbc._
import ulysses.util.LocalPublishing
import scuff.ddd.DuplicateIdException
import scuff.concurrent._
import ulysses.EventSource
import ulysses.StreamFilter
import ulysses.StreamFilter.ByEvent
import sampler.aggr.dept.EmployeeAdded
import sampler.aggr.emp.EmployeeRegistered
import com.mongodb.async.client.MongoClient
import ulysses.mongo.MongoEventStore
import com.mongodb.async.SingleResultCallback
import scala.concurrent.Promise
import org.bson.codecs.configuration.CodecRegistry

object TestSampler {
  val h2Name = "h2db"
  val h2File = new File(".", h2Name + ".mv.db")
  @AfterClass
  def cleanup {
    if (!h2File.delete()) {
      println("Failed to delete: " + h2File.getCanonicalPath)
    }
  }
}

class TestSampler {

  val isDebug = java.lang.management.ManagementFactory
    .getRuntimeMXBean
    .getInputArguments
    .toString.contains("jdwp")

  val AwaitDuration = if (isDebug) 10.hours else 10.seconds

  var es: EventSource[UUID, DomainEvent, Aggregate.Root] = _
  var EmployeeRepo: Repository[EmpId, Employee] = _
  var DepartmentRepo: Repository[DeptId, Department] = _

  @Before
  def setupMongo {
    import ulysses.mongo._
    import com.mongodb.async.client._
    val dbName = (1 to 10).map(_ => Random.nextInRange('a' to 'z')).mkString
    val client = MongoClients.create()
    val db = client.getDatabase(dbName)
    val txnCollection = db.getCollection("event-store").withCodec(RootBsonCodec)
    //    val promise = Promise[Unit]
    //    val txnCollection = db.createCollection("event-store", new SingleResultCallback[Void] {
    //      def onResult(void: Void, th: Throwable) {
    //        if (th == null) promise.success(Unit)
    //        else promise failure th
    //      }
    //    })
    //    promise.future.await

    val mongoES =
      new MongoEventStore[UUID, DomainEvent, Aggregate.Root](
        txnCollection) with LocalPublishing[UUID, DomainEvent, Aggregate.Root] {
        def publishCtx = implicitly[ExecutionContext]
      }
    val clock = LamportClock(mongoES)
    EmployeeRepo = new EntityRepository(clock, Aggregate.Employee)(mongoES)
    DepartmentRepo = new EntityRepository(clock, Aggregate.Department)(mongoES)
    es = mongoES
  }

  //@Before
  def setupH2 {
    import ulysses.jdbc.h2._
    //    val schema = (1 to 10).map(_ => Random.nextInRange('a' to 'z')).mkString
    val sql = new Dialect[UUID, DomainEvent, Aggregate.Root, JSON](None)
    val ds = new JdbcDataSource
    ds.setURL(s"jdbc:h2:./${TestSampler.h2Name}")
    //    ds.setURL(s"jdbc:h2:mem:test")
    val jdbcES = new JdbcEventStore(ds, sql) with LocalPublishing[UUID, DomainEvent, Aggregate.Root] {
      def publishCtx = implicitly[ExecutionContext]
    }
    val clock = LamportClock(jdbcES)
    EmployeeRepo = new EntityRepository(clock, Aggregate.Employee)(jdbcES)
    DepartmentRepo = new EntityRepository(clock, Aggregate.Department)(jdbcES)
    es = jdbcES
  }

//  implicit def any2future[T](any: T): Future[T] = any match {
//    case f: Future[T] => f
//    case _ => Future successful any
//  }

  @Test
  def inserting {
    val id = new EmpId
    assertTrue(EmployeeRepo.exists(id).await.isEmpty)
    val register = RegisterEmployee("John Doe", "555-55-5555", new MyDate(1988, 4, 1), 43000, "Janitor")
    val emp = Employee(register)
    val insertRev = EmployeeRepo.insert(id, emp).await
    assertEquals(0, insertRev)
    Try(EmployeeRepo.insert(id, emp).await) match {
      case Success(revision) =>
        // Allow idempotent inserts
        assertEquals(0, revision)
      case Failure(th) =>
        fail(s"Should succeed, but didn't: $th")
    }
    emp.apply(UpdateSalary(40000))
    Try(EmployeeRepo.insert(id, emp).await) match {
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
    val insertRev = EmployeeRepo.insert(id, emp).await
    assertEquals(0, insertRev)
    @volatile var updateRev = -1
    var updatedRev = EmployeeRepo.update(id, 0) {
      case (emp, revision) =>
        updateRev = revision
        Future successful emp(UpdateSalary(45000))
    }.await
    assertEquals(0, updateRev)
    assertEquals(1, updatedRev)
    updatedRev = EmployeeRepo.update(id, 1) {
      case (emp, revision) =>
        updateRev = revision
        Future successful emp(UpdateSalary(45000))
    }.await
    assertEquals(1, updateRev)
    assertEquals(1, updatedRev)
  }

  private def register(id: EmpId, cmd: RegisterEmployee): Employee = {
    val emp = Employee(cmd)
    EmployeeRepo.insert(id, emp).await
    emp
  }

  @Test
  def replay {
    val jdId = new EmpId
    assertTrue(EmployeeRepo.exists(jdId).await.isEmpty)
    register(jdId, RegisterEmployee("John Doe", "555-55-5555", new MyDate(1988, 4, 1), 43000, "Janitor"))
    val djId = new EmpId
    assertTrue(EmployeeRepo.exists(djId).await.isEmpty)
    register(djId, RegisterEmployee("Don Joe", "666-66-6666", new MyDate(1992, 1, 31), 61000, "Developer"))
    assertEquals(0, Department.insert(DepartmentRepo)(new DeptId, CreateDepartment("Silly Walks"))(_ => Map()).await)
    val dodId = new DeptId
    val deptRev = Department.insert(DepartmentRepo)(dodId, CreateDepartment("DoD"))(_ => Map.empty).await
    assertEquals(0, deptRev)
    val rev1 = DepartmentRepo.update(dodId, 0) {
      case (dept, rev) =>
        Future successful dept(AddEmployee(jdId))
    }
    assertEquals(1, rev1.await)
    val rev2 = DepartmentRepo.update(dodId, 1) {
      case (dept, rev) =>
        Future successful dept(AddEmployee(djId))
    }
    assertEquals(2, rev2.await)
    val filter = ByEvent[UUID, DomainEvent, Aggregate.Root] {
      Map(
        Aggregate.Employee -> Set(classOf[EmployeeRegistered]),
        Aggregate.Department -> Set(classOf[EmployeeAdded]))
    }
    val footure = StreamPromise.fold(es.replay(filter))(Map.empty[EmpId, String] -> Map.empty[DeptId, List[String]]) {
      case (tuple, txn) => txn.events.foldLeft(tuple) {
        case ((employees, depts), EmployeeRegistered(name, _, _, _, _)) =>
          employees.updated(EmpId(txn.stream), name) -> depts
        case ((employees, depts), EmployeeAdded(empId)) =>
          val deptId = DeptId(txn.stream)
          val nameList = employees(empId) :: depts.getOrElse(deptId, Nil)
          employees -> depts.updated(deptId, nameList)
        case (tuple, _) => tuple
      }
    }
    val (_, empByDept) = footure.await
    assertEquals(1, empByDept.size)
    val employees = empByDept(dodId)
    assertEquals(2, employees.size)
    assertTrue(employees.contains("John Doe"))
    assertTrue(employees.contains("Don Joe"))
  }
}
