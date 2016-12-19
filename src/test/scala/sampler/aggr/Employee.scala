package sampler.aggr

import sampler.aggr.emp.EmpEvent
import ulysses.ddd.StateMutator
import sampler.aggr.emp.EmployeeRegistered
import sampler._
import sampler.aggr.emp.EmployeeSalaryChange
import sampler.aggr.emp.EmployeeTitleChange
import ulysses.ddd.StateMutator
import ulysses.ddd.AggregateRoot

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

  def apply(cmd: RegisterEmployee): Employee = {
    val name = cmd.name.trim
    require(name.length() > 0)
    require("""\d{3}-\d{2}-\d{4}""".r.pattern.matcher(cmd.soch).matches)
    val title = cmd.title.trim
    require(title.length > 0)
    val emp = new Employee
    emp.mutator(EmployeeRegistered(name, cmd.soch, cmd.dob, cmd.annualSalary, title))
    emp
  }

  object Def extends AggregateRoot {
    type Id = EmpId
    type Channel = Aggr.Value
    def channel = Aggr.Empl
    type Entity = Employee
    type Event = emp.EmpEvent
    type State = emp.State
    def newMutator(state: Option[State]) = new emp.Mutator(state)
    def init(state: State, mergeEvents: List[Event]) = new Employee(new emp.Mutator(state), mergeEvents)
    def done(employee: Employee) = employee.mutator
    def checkInvariants(state: State): Unit = ()
  }

}

class Employee private[aggr] (
    private[aggr] val mutator: emp.Mutator = new emp.Mutator,
    mergeEvents: Seq[EmpEvent] = Nil) {
  @inline private def emp = mutator.state

  def apply(cmd: UpdateSalary): this.type =  {
    checkAndUpdateSalary(cmd.newSalary)
    assert(cmd.newSalary == emp.salary)
    this
  }
  private def checkAndUpdateSalary(newSalary: Int) {
    require(newSalary > 0)
    if (newSalary != emp.salary) {
      mutator(EmployeeSalaryChange(newSalary))
    }

    assert(newSalary == emp.salary)
  }
  def apply(cmd: PromoteEmployee): this.type = {
    val title = cmd.newTitle.trim
    require(title.length > 0)
    checkAndUpdateSalary(cmd.newSalary)
    mutator(EmployeeTitleChange(title))

    assert(cmd.newSalary == emp.salary)
    assert(cmd.newTitle == emp.title)

    this
  }
}
