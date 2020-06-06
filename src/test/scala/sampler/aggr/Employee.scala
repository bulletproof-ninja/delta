package sampler.aggr

import delta.write._

import sampler._
import sampler.aggr.emp._

case class RegisterEmployee(
  name: String,
  soch: String,
  dob: MyDate,
  annualSalary: Int,
  title: String)
case class UpdateSalary(
  newSalary: Int)
case class PromoteEmployee(
  newSalary: Int,
  newTitle: String)

object Employee {

  type State = delta.write.State[EmpState, EmpEvent]

  def apply(cmd: RegisterEmployee): Employee = {
    val name = cmd.name.trim
    require(name.length() > 0)
    require("""\d{3}-\d{2}-\d{4}""".r.pattern.matcher(cmd.soch).matches)
    val title = cmd.title.trim
    require(title.length > 0)
    val emp = new Employee
    emp.state(EmployeeRegistered(name, cmd.soch, cmd.dob, cmd.annualSalary, title))
    emp
  }

  object Def extends Entity("Employee", EmpProjector) {
    type Id = EmpId
    type Type = Employee
    def init(state: State, concurrentUpdates: List[Transaction]) = new Employee(state)
    def state(employee: Employee) = employee.state
    def validate(state: EmpState) = require(state != null)
  }

}

class Employee private[aggr] (
    private[aggr] val state: Employee.State = Employee.Def.newState()) {
  @inline private def emp = state.get

  def apply(cmd: UpdateSalary): this.type =  {
    checkAndUpdateSalary(cmd.newSalary)
    assert(cmd.newSalary == emp.salary)
    this
  }
  private def checkAndUpdateSalary(newSalary: Int): Unit = {
    require(newSalary > 0)
    if (newSalary != emp.salary) {
      state(EmployeeSalaryChange(newSalary))
    }

    assert(newSalary == emp.salary)
  }
  def apply(cmd: PromoteEmployee): this.type = {
    val title = cmd.newTitle.trim
    require(title.length > 0)
    checkAndUpdateSalary(cmd.newSalary)
    state(EmployeeTitleChange(title))

    assert(cmd.newSalary == emp.salary)
    assert(cmd.newTitle == emp.title)

    this
  }
}
