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

  type StateRef = delta.write.StateRef[EmpState, EmpEvent]

  def apply(cmd: RegisterEmployee): Employee = {
    val name = cmd.name.trim
    require(name.length() > 0)
    require("""\d{3}-\d{2}-\d{4}""".r.pattern.matcher(cmd.soch).matches)
    val title = cmd.title.trim
    require(title.length > 0)
    val emp = new Employee
    emp.stateRef apply EmployeeRegistered(name, cmd.soch, cmd.dob, cmd.annualSalary, title)
    emp
  }

  object Def extends Entity("Employee", EmpProjector) {
    type Id = EmpId
    type Type = Employee
    def init(id: Id, state: StateRef, concurrentUpdates: List[Transaction]) = new Employee(state)
    def StateRef(employee: Employee) = employee.stateRef
    def validate(state: EmpState) = require(state != null)
  }

}

class Employee private[aggr] (
    private[aggr] val stateRef: Employee.StateRef = Employee.Def.newStateRef()) {
  @inline private def emp = stateRef.get

  def apply(cmd: UpdateSalary): this.type =  {
    checkAndUpdateSalary(cmd.newSalary)
    assert(cmd.newSalary == emp.salary)
    this
  }
  private def checkAndUpdateSalary(newSalary: Int): Unit = {
    require(newSalary > 0)
    if (newSalary != emp.salary) {
      stateRef apply EmployeeSalaryChange(newSalary)
    }

    assert(newSalary == emp.salary)
  }
  def apply(cmd: PromoteEmployee): this.type = {
    val title = cmd.newTitle.trim
    require(title.length > 0)
    checkAndUpdateSalary(cmd.newSalary)
    stateRef apply EmployeeTitleChange(title)

    assert(cmd.newSalary == emp.salary)
    assert(cmd.newTitle == emp.title)

    this
  }
}
