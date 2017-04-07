package sampler.aggr.dept

import sampler._
import delta.ddd.StateMutator
import delta.Fold

case class State(
  name: String,
  employees: Set[EmpId] = Set.empty)

private class StateHandler(state: State = null)
    extends DeptEventHandler {

  type RT = State

  def on(evt: DeptCreated): RT = {
    require(state == null)
    new State(evt.name)
  }
  def on(evt: EmployeeAdded): RT = {
    state.copy(employees = state.employees + evt.id)
  }
  def on(evt: EmployeeRemoved): RT = {
    state.copy(employees = state.employees - evt.id)
  }
  def on(evt: NameChanged): RT = {
    state.copy(name = evt.newName)
  }

}

private[aggr] class Mutator
    extends StateMutator {

  protected val fold = new Fold[State, DeptEvent] {
    def init(evt: DeptEvent): State = new StateHandler().dispatch(evt)
    def next(state: State, evt: DeptEvent): State = new StateHandler(state).dispatch(evt)
  }

  type Event = DeptEvent
  type State = sampler.aggr.dept.State

}
