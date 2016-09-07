package ulysses.test_repo.aggr.emp

import ulysses.test_repo.aggr.EmpId
import ulysses.ddd.StateMutator
import ulysses.test_repo.LocalDate

case class State(
  name: String,
  soch: String,
  dob: LocalDate,
  salary: Int,
  title: String)

class Mutator(
  var state: State = null)
    extends StateMutator[EmpEvent, State]
    with EmpEventHandler {

  type RT = Unit

  def this(state: Option[State]) = this(state.orNull)

  /** Genesis event. */
  def on(evt: EmployeeRegistered): RT = {
    require(state == null)
  }

  def on(evt: EmployeeSalaryChange): RT = {
    state = state.copy(salary = evt.newSalary)
  }

  def on(evt: EmployeeTitleChange): RT = {
    state = state.copy(title = evt.newTitle)
  }

}
