package sampler.aggr.dept

import rapture.json._, jsonBackends.jackson._
import java.util.UUID
import sampler.aggr.EmpId
import ulysses.util.ReflectiveDecoder

trait JsonCodec
    extends DeptEventHandler {
  this: ReflectiveDecoder[_, String] =>

  type RT = String

  def on(evt: DeptCreated): RT = json"""{
  "name": ${evt.name}
}""".toBareString
  def onDeptCreated(version: Short, json: String): DeptCreated = version match {
    case 1 => Json.parse(json) match {
      case json""" { "name": $name } """ => new DeptCreated(name = name.as[String])
    }
  }

  def on(evt: EmployeeAdded): RT = json"""{
  "employee": ${evt.id.uuid.toString}
}""".toBareString
  def onEmployeeAdded(version: Short, json: String): EmployeeAdded = version match {
    case 1 => Json.parse(json) match {
      case json""" { "employee": $uuid } """ =>
        new EmployeeAdded(id = new EmpId(UUID fromString uuid.as[String]))
    }
  }

  def on(evt: EmployeeRemoved): RT = json"""{
  "employee": ${evt.id.uuid.toString}
}""".toBareString
  def onEmployeeRemoved(version: Short, json: String): EmployeeRemoved = version match {
    case 1 => Json.parse(json) match {
      case json""" { "employee": $uuid } """ =>
        new EmployeeRemoved(id = new EmpId(UUID fromString uuid.as[String]))
    }
  }

  def on(evt: NameChanged): RT = json"""{
  "name": ${evt.newName},
  "reason": ${evt.reason}
}""".toBareString
  def onNameChanged(version: Short, json: String): NameChanged = version match {
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
