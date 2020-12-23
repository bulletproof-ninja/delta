package sampler.aggr.dept

import sampler._
import delta.util.ReflectiveDecoder
import scuff.json._, JsVal._

trait JsonCodec
    extends DeptEventHandler {
  this: ReflectiveDecoder[_, String] =>

  type Return = JSON

  def on(evt: DeptCreated): Return = JsStr(evt.name).toJson
  def onDeptCreated(enc: Encoded): DeptCreated = enc.version {
    case 1 => new DeptCreated(name = JsVal.parse(enc.data).asStr)
  }

  def on(evt: EmployeeAdded): Return = evt.id.int.toString
  def onEmployeeAdded(enc: Encoded): EmployeeAdded = enc.version {
    case 1 => new EmployeeAdded(id = new EmpId(enc.data.toInt))
  }

  def on(evt: EmployeeRemoved): Return = evt.id.int.toString
  def onEmployeeRemoved(enc: Encoded): EmployeeRemoved = enc.version {
    case 1 => new EmployeeRemoved(id = new EmpId(enc.data.toInt))
  }

  def on(evt: NameChanged): Return = JsVal(evt).toJson
  def onNameChanged(enc: Encoded): NameChanged = {
    val jsObj = JsVal.parse(enc.data).asObj
    enc.version {
      case 1 => new archive.NameChanged_v1(newName = jsObj.newName.asStr).upgrade()
      case 2 => new NameChanged(newName = jsObj.newName.asStr, reason = jsObj.reason.asStr)
    }
  }

}
