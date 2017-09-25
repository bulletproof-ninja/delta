package sampler.aggr.emp

import sampler.MyDate
import sampler.aggr.DomainEvent

trait EmpEventHandler {
  type Return

  def dispatch(evt: EmpEvent) = evt.dispatch(this)

  def on(evt: EmployeeRegistered): Return
  def on(evt: EmployeeSalaryChange): Return
  def on(evt: EmployeeTitleChange): Return
}

sealed abstract class EmpEvent extends DomainEvent {
  type Callback = EmpEventHandler
}

@SerialVersionUID(1)
case class EmployeeRegistered(
  name: String,
  soch: String,
  dob: MyDate,
  annualSalary: Int,
  title: String)
    extends EmpEvent { def dispatch(handler: EmpEventHandler): handler.Return = handler.on(this) }

@SerialVersionUID(1)
case class EmployeeSalaryChange(newSalary: Int)
  extends EmpEvent { def dispatch(handler: EmpEventHandler): handler.Return = handler.on(this) }

@SerialVersionUID(1)
case class EmployeeTitleChange(newTitle: String)
  extends EmpEvent { def dispatch(handler: EmpEventHandler): handler.Return = handler.on(this) }
