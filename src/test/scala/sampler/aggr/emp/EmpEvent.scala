package sampler.aggr.emp

import scuff.DoubleDispatch
import sampler.MyDate
import sampler.aggr.DomainEvent

trait EmpEventHandler {
  def apply(evt: EmpEvent) = evt.dispatch(this)

  type RT

  def on(evt: EmployeeRegistered): RT
  def on(evt: EmployeeSalaryChange): RT
  def on(evt: EmployeeTitleChange): RT
}

sealed abstract class EmpEvent
  extends DomainEvent
  with DoubleDispatch[EmpEventHandler]

@SerialVersionUID(1)
case class EmployeeRegistered(
  name: String,
  soch: String,
  dob: MyDate,
  annualSalary: Int,
  title: String)
    extends EmpEvent { def dispatch(handler: EmpEventHandler): handler.RT = handler.on(this) }

@SerialVersionUID(1)
case class EmployeeSalaryChange(newSalary: Int)
  extends EmpEvent { def dispatch(handler: EmpEventHandler): handler.RT = handler.on(this) }

@SerialVersionUID(1)
case class EmployeeTitleChange(newTitle: String)
  extends EmpEvent { def dispatch(handler: EmpEventHandler): handler.RT = handler.on(this) }
