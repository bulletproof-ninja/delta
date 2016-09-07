package ulysses.test_repo.aggr

import ulysses.test_repo.aggr.emp.EmpEvent

trait Department {
  def apply(cmd: AddEmployee)
  def apply(cmd: RemoveEmployee)
}

/** Genesis command. */
case class CreateDepartment(name: String)
case class AddEmployee(id: EmpId)
case class RemoveEmployee(id: EmpId)

object Department {

  def apply(cmd: CreateDepartment): Department = {
    val mutator = new dept.Mutator()

  }

  class Impl(mutator: dept.Mutator, concurrentEvents: List[EmpEvent])
    extends Department {

  }
}
