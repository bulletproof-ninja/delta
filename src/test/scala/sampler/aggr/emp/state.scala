package sampler.aggr.emp

import sampler._
import delta.Projector

case class EmpState(
    name: String,
    soch: String,
    dob: MyDate,
    salary: Int,
    title: String)

private class StateHandler(state: EmpState = null)
  extends EmpEventHandler {

  type Return = EmpState

  def on(evt: EmployeeRegistered): Return = {
    require(state == null)
    EmpState(name = evt.name, soch = evt.soch, dob = evt.dob, salary = evt.annualSalary, title = evt.title)
  }

  def on(evt: EmployeeSalaryChange): Return = {
    state.copy(salary = evt.newSalary)
  }

  def on(evt: EmployeeTitleChange): Return = {
    state.copy(title = evt.newTitle)
  }

}

object EmpProjector extends Projector[EmpState, EmpEvent] {
  def init(evt: EmpEvent) = new StateHandler().dispatch(evt)
  def next(state: EmpState, evt: EmpEvent) = new StateHandler(state).dispatch(evt)
}
