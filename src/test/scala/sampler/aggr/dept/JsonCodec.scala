package sampler.aggr.dept

import rapture.json._, jsonBackends.jackson._
import sampler._
import delta.util.ReflectiveDecoder

trait JsonCodec
    extends DeptEventHandler {
  this: ReflectiveDecoder[_, String] =>

  type Return = JSON

  def on(evt: DeptCreated): Return = json"""{
  "name": ${evt.name}
}""".toBareString
  def onDeptCreated(version: Byte, json: String): DeptCreated = version match {
    case 1 => Json.parse(json) match {
      case json""" { "name": $name } """ => new DeptCreated(name = name.as[String])
    }
  }

  def on(evt: EmployeeAdded): Return = json"""{
  "employee": ${evt.id.int}
}""".toBareString
  def onEmployeeAdded(version: Byte, json: String): EmployeeAdded = version match {
    case 1 => Json.parse(json) match {
      case json""" { "employee": $intId } """ =>
        new EmployeeAdded(id = new EmpId(intId.as[Int]))
    }
  }

  def on(evt: EmployeeRemoved): Return = json"""{
  "employee": ${evt.id.int}
}""".toBareString
  def onEmployeeRemoved(version: Byte, json: String): EmployeeRemoved = version match {
    case 1 => Json.parse(json) match {
      case json""" { "employee": $intId } """ =>
        new EmployeeRemoved(id = new EmpId(intId.as[Int]))
    }
  }

  def on(evt: NameChanged): Return = json"""{
  "name": ${evt.newName},
  "reason": ${evt.reason}
}""".toBareString
  def onNameChanged(version: Byte, json: String): NameChanged = version match {
    case 1 => Json.parse(json) match {
      case json""" { "name": $name } """ =>
        new archive.NameChanged_v1(newName = name.as[String]).upgrade()
    }
    case 2 => Json.parse(json) match {
      case json""" { "reason": $reason, "name": $name } """ =>
        new NameChanged(newName = name.as[String], reason = reason.as[String])
    }
  }

}
