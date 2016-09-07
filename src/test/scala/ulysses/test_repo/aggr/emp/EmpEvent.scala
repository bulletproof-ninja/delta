package ulysses.test_repo.aggr.emp

import scuff.DoubleDispatch
import ulysses.test_repo.LocalDate

trait EmpEventHandler {
  type RT

  def apply(evt: EmpEvent) = evt.dispatch(this)

  def on(evt: EmployeeRegistered): RT
  def on(evt: EmployeeSalaryChange): RT
  def on(evt: EmployeeTitleChange): RT
}

sealed abstract class EmpEvent extends DoubleDispatch[EmpEventHandler]

@SerialVersionUID(1)
case class EmployeeRegistered(
  name: String,
  soch: String,
  dob: LocalDate,
  annualSalary: Int,
  title: String)
  extends EmpEvent { def dispatch(handler: EmpEventHandler): handler.RT = handler.on(this) }

@SerialVersionUID(1)
case class EmployeeSalaryChange(newSalary: Int)
  extends EmpEvent { def dispatch(handler: EmpEventHandler): handler.RT = handler.on(this) }

@SerialVersionUID(1)
case class EmployeeTitleChange(newTitle: String)
  extends EmpEvent { def dispatch(handler: EmpEventHandler): handler.RT = handler.on(this) }
