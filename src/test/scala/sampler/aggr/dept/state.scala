package sampler.aggr.dept

import sampler._
import delta.EventReducer

case class DeptState(
    name: String,
    employees: Set[EmpId] = Set.empty)

private class StateHandler(state: DeptState = null)
  extends DeptEventHandler {

  type Return = DeptState

  def on(evt: DeptCreated): Return = {
    require(state == null)
    new DeptState(evt.name)
  }
  def on(evt: EmployeeAdded): Return = {
    state.copy(employees = state.employees + evt.id)
  }
  def on(evt: EmployeeRemoved): Return = {
    state.copy(employees = state.employees - evt.id)
  }
  def on(evt: NameChanged): Return = {
    state.copy(name = evt.newName)
  }

}

object DeptAssembler extends EventReducer[DeptState, DeptEvent] {
  def init(evt: DeptEvent): DeptState = new StateHandler().dispatch(evt)
  def next(state: DeptState, evt: DeptEvent): DeptState = new StateHandler(state).dispatch(evt)
}
