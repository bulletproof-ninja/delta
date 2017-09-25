package sampler.aggr.dept

import scala.{ SerialVersionUID => version }
import sampler._
import sampler.aggr.DomainEvent
import scuff.ReplacedBy

trait DeptEventHandler {
  type Return

  def dispatch(evt: DeptEvent) = evt.dispatch(this)

  def on(evt: DeptCreated): Return
  def on(evt: EmployeeAdded): Return
  def on(evt: EmployeeRemoved): Return
  def on(evt: NameChanged): Return
}

sealed abstract class DeptEvent extends DomainEvent {
  type Callback = DeptEventHandler
}

@version(1)
case class DeptCreated(name: String)
  extends DeptEvent { def dispatch(handler: DeptEventHandler): handler.Return = handler.on(this) }

@version(1)
case class EmployeeAdded(id: EmpId)
  extends DeptEvent { def dispatch(handler: DeptEventHandler): handler.Return = handler.on(this) }

@version(1)
case class EmployeeRemoved(id: EmpId)
  extends DeptEvent { def dispatch(handler: DeptEventHandler): handler.Return = handler.on(this) }

@version(2)
case class NameChanged(newName: String, reason: String)
  extends DeptEvent { def dispatch(handler: DeptEventHandler): handler.Return = handler.on(this) }

package archive {
  @version(1)
  case class NameChanged_v1(newName: String) extends ReplacedBy[NameChanged] {
    def upgrade() = new NameChanged(newName = newName, reason = "N/A")
  }

}
