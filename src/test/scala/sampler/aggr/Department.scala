package sampler.aggr

import delta.write._

import sampler._
import sampler.aggr.dept._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext

/** Genesis command. */
case class CreateDepartment(name: String)
case class AddEmployee(id: EmpId)
case class RemoveEmployee(id: EmpId)

trait Department {
  def apply(cmd: AddEmployee): this.type
  def apply(cmd: RemoveEmployee): this.type
}

object Department {
  type StateRef = delta.write.StateRef[DeptState, DeptEvent]

  def insert(repo: Repository[DeptId, Department])(
      id: DeptId, cmd: CreateDepartment)(
      thunk: Department => Metadata)(
      implicit ec: ExecutionContext)
      : Future[Int] = {
    val name = cmd.name.trim()
    require(name.length() > 0)
    val dept = new Impl
    dept.state(DeptCreated(name))
    implicit val metadata = thunk(dept)
    repo.insert(id, dept).map(_ => 0)
  }

  object Def extends Entity("Department", DeptProjector) {
    type Id = DeptId
    type Type = Department
    def init(id: Id, state: StateRef, concurrentUpdates: List[Transaction]) = new Impl(state)
    def StateRef(dept: Department) = dept match {
      case dept: Impl => dept.state
      case _ => ???
    }
    def validate(state: DeptState) = require(state != null)
  }

  private[aggr] class Impl(val state: StateRef = Def.newStateRef())
      extends Department {
    @inline
    private def dept = state.get

    def apply(cmd: AddEmployee) = {
      if (!dept.employees(cmd.id)) {
        state(EmployeeAdded(cmd.id))
      }
      this
    }
    def apply(cmd: RemoveEmployee) = {
      if (dept.employees(cmd.id)) {
        state(EmployeeRemoved(cmd.id))
      }
      this
    }

  }

}
