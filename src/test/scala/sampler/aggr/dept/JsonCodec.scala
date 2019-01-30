package sampler.aggr.dept

import sampler._
import delta.util.ReflectiveDecoder
import delta.testing._

trait JsonCodec
    extends DeptEventHandler {
  this: ReflectiveDecoder[_, String] =>

  type Return = JSON

  def on(evt: DeptCreated): Return = s"""{
    "name": "${evt.name}"
  }"""
  def onDeptCreated(enc: Encoded): DeptCreated = enc.version match {
    case 1 => new DeptCreated(name = enc.data.field("name"))
  }

  def on(evt: EmployeeAdded): Return = s"""{
    "employee": ${evt.id.int}
  }"""
  def onEmployeeAdded(enc: Encoded): EmployeeAdded = enc.version match {
    case 1 => new EmployeeAdded(id = new EmpId(enc.data.field("employee").toInt))
  }

  def on(evt: EmployeeRemoved): Return = s"""{
    "employee": ${evt.id.int}
  }"""
  def onEmployeeRemoved(enc: Encoded): EmployeeRemoved = enc.version match {
    case 1 => new EmployeeRemoved(id = new EmpId(enc.data.field("employee").toInt))
  }

  def on(evt: NameChanged): Return = s"""{
    "name": "${evt.newName}",
    "reason": "${evt.reason}"
  }"""
  def onNameChanged(enc: Encoded): NameChanged = enc.version match {
    case 1 => new archive.NameChanged_v1(newName = enc.data.field("name")).upgrade()
    case 2 => new NameChanged(newName = enc.data.field("name"), reason = enc.data.field("reason"))
  }

}
