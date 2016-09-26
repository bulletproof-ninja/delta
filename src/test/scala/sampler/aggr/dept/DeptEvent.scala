package sampler.aggr.dept

import scala.{ SerialVersionUID => version }
import scuff.DoubleDispatch
import sampler.aggr.EmpId
import sampler.aggr.DomainEvent
import scuff.ReplacedBy

trait DeptEventHandler extends (DeptEvent => Any) {
  type RT

  def apply(evt: DeptEvent) = evt.dispatch(this)

  def on(evt: DeptCreated): RT
  def on(evt: EmployeeAdded): RT
  def on(evt: EmployeeRemoved): RT
  def on(evt: NameChanged): RT
}

sealed abstract class DeptEvent
  extends DomainEvent
  with DoubleDispatch[DeptEventHandler]

@version(1)
case class DeptCreated(name: String)
  extends DeptEvent { def dispatch(handler: DeptEventHandler): handler.RT = handler.on(this) }

@version(1)
case class EmployeeAdded(id: EmpId)
  extends DeptEvent { def dispatch(handler: DeptEventHandler): handler.RT = handler.on(this) }

@version(1)
case class EmployeeRemoved(id: EmpId)
  extends DeptEvent { def dispatch(handler: DeptEventHandler): handler.RT = handler.on(this) }

@version(2)
case class NameChanged(newName: String, reason: String)
  extends DeptEvent { def dispatch(handler: DeptEventHandler): handler.RT = handler.on(this) }

package archive {
  @version(1)
  case class NameChanged_v1(newName: String) extends ReplacedBy[NameChanged] {
    def upgrade() = new NameChanged(newName = newName, reason = "N/A")
  }

}
