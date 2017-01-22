package sampler.aggr.emp

import sampler._
import delta.ddd.StateMutator
import delta.Fold

case class State(
  name: String,
  soch: String,
  dob: MyDate,
  salary: Int,
  title: String)

private class StateHandler(state: State = null)
    extends EmpEventHandler {

  type RT = State

  def on(evt: EmployeeRegistered): RT = {
    require(state == null)
    State(name = evt.name, soch = evt.soch, dob = evt.dob, salary = evt.annualSalary, title = evt.title)
  }

  def on(evt: EmployeeSalaryChange): RT = {
    state.copy(salary = evt.newSalary)
  }

  def on(evt: EmployeeTitleChange): RT = {
    state.copy(title = evt.newTitle)
  }

}

private[aggr] class Mutator extends StateMutator {

  type Event = EmpEvent
  type State = sampler.aggr.emp.State

  protected val fold = new Fold[State, Event] {
    def init(evt: Event) = new StateHandler().dispatch(evt)
    def next(state: State, evt: Event) = new StateHandler(state).dispatch(evt)
  }

}
