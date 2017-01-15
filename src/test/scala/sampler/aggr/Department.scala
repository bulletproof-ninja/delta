package sampler.aggr

import collection.immutable.Seq
import scuff.ddd._
import delta.ddd._

import sampler._
import sampler.aggr.dept._
import scala.concurrent.Future

/** Genesis command. */
case class CreateDepartment(name: String)
case class AddEmployee(id: EmpId)
case class RemoveEmployee(id: EmpId)

trait Department {
  def apply(cmd: AddEmployee): this.type
  def apply(cmd: RemoveEmployee): this.type
}

object Department {

  def insert(repo: Repository[DeptId, Department])(
    id: DeptId, cmd: CreateDepartment)(
      thunk: Department => Map[String, String]): Future[Int] = {
    val name = cmd.name.trim()
    require(name.length() > 0)
    val dept = new Impl
    dept.mutator(DeptCreated(name))
    val metadata = thunk(dept)
    repo.insert(id, dept, metadata)
  }

  object Def extends Entity {
    type Id = DeptId
    type Type = Department
    type Mutator = dept.Mutator
    def newMutator = new Mutator
    def init(state: Mutator, mergeEvents: List[DeptEvent]) = new Impl(state, mergeEvents)
    def done(dept: Department) = dept match {
      case dept: Impl => dept.mutator
    }
  }

  private[aggr] class Impl(val mutator: dept.Mutator = new dept.Mutator, mergeEvents: Seq[DeptEvent] = Nil)
      extends Department {
    @inline
    private def state = mutator.state

    def apply(cmd: AddEmployee) = {
      if (!state.employees(cmd.id)) {
        mutator(EmployeeAdded(cmd.id))
      }
      this
    }
    def apply(cmd: RemoveEmployee) = {
      if (state.employees(cmd.id)) {
        mutator(EmployeeRemoved(cmd.id))
      }
      this
    }

  }

}
