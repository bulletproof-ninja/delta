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
  def onDeptCreated(version: Byte, json: String): DeptCreated = version match {
    case 1 => new DeptCreated(name = json.field("name"))
  }

  def on(evt: EmployeeAdded): Return = s"""{
    "employee": ${evt.id.int}
  }"""
  def onEmployeeAdded(version: Byte, json: String): EmployeeAdded = version match {
    case 1 => new EmployeeAdded(id = new EmpId(json.field("employee").toInt))
  }

  def on(evt: EmployeeRemoved): Return = s"""{
    "employee": ${evt.id.int}
  }"""
  def onEmployeeRemoved(version: Byte, json: String): EmployeeRemoved = version match {
    case 1 => new EmployeeRemoved(id = new EmpId(json.field("employee").toInt))
  }

  def on(evt: NameChanged): Return = s"""{
    "name": "${evt.newName}",
    "reason": "${evt.reason}"
  }"""
  def onNameChanged(version: Byte, json: String): NameChanged = version match {
    case 1 => new archive.NameChanged_v1(newName = json.field("name")).upgrade()
    case 2 => new NameChanged(newName = json.field("name"), reason = json.field("reason"))
  }

}
