package ulysses.test_repo.aggr.dept

import scala.{ SerialVersionUID => version }
import scuff.DoubleDispatch
import ulysses.EventContext
import ulysses.test_repo.aggr.EmpId

trait DeptEventHandler {
  type RT

  def apply(evt: DeptEvent) = evt.dispatch(this)

  def on(evt: DeptCreated): RT
  def on(evt: EmployeeAdded): RT
  def on(evt: EmployeeRemoved): RT
  def on(evt: NameChanged): RT
}

sealed abstract class DeptEvent extends DoubleDispatch[DeptEventHandler]

@version(1)
case class DeptCreated(name: String)
  extends DeptEvent { def dispatch(handler: DeptEventHandler): handler.RT = handler.on(this) }

@version(1)
case class EmployeeAdded(id: EmpId)
  extends DeptEvent { def dispatch(handler: DeptEventHandler): handler.RT = handler.on(this) }

@version(1)
case class EmployeeRemoved(id: EmpId)
  extends DeptEvent { def dispatch(handler: DeptEventHandler): handler.RT = handler.on(this) }

@version(1)
case class NameChanged(newName: String)
  extends DeptEvent { def dispatch(handler: DeptEventHandler): handler.RT = handler.on(this) }

//object DeptEvent
//    extends EventContext[DeptEvent, Unit, String]
//    with DeptEventHandler {
//
//  def version(evt: Class[DeptEvent]): Short = scuff.serialVersionUID(evt).toShort
//  def name(evt: Class[DeptEvent]): String = evt.getSimpleName
//  def channel(evt: Class[DeptEvent]) = Unit
//
//  def encode(evt: DeptEvent): String = ""
//  def decode(name: String, version: Short, data: String): DeptEvent = {
//
//  }
//
//  type RT = DeptEvent
//
//}
