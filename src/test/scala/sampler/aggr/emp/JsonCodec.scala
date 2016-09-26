package sampler.aggr.emp

import rapture.json._, jsonBackends.jackson._
import java.util.UUID
import ulysses.util.ReflectiveDecoder
import sampler.MyDate

trait JsonCodec
    extends EmpEventHandler {
  this: ReflectiveDecoder[_, String] =>

  type RT = String

  private val IsoDate = """(\d{4})-(\d{2})-(\d{2})""".r

  def on(evt: EmployeeRegistered): String = json"""{
  "name": ${evt.name},
  "dob": ${evt.dob.toString},
  "ssn": ${evt.soch},
  "title": ${evt.title},
  "salary": ${evt.annualSalary}
}""".toBareString
  def onEmployeeRegistered(version: Short, json: String): EmployeeRegistered = version match {
    case 1 => Json.parse(json) match {
      case json"""{
  "name": $name,
  "dob": $dobStr,
  "ssn": $ssn,
  "title": $title,
  "salary": $salary
}""" =>
        val IsoDate(year, month, day) = dobStr.as[String]
        new EmployeeRegistered(
          name = name.as[String],
          dob = new MyDate(year.toShort, month.toByte, day.toByte),
          soch = ssn.as[String],
          title = title.as[String],
          annualSalary = salary.as[Int])
    }
  }
  def on(evt: EmployeeSalaryChange): String = json"""{
  "salary": ${evt.newSalary}
}""".toBareString
  def onEmployeeSalaryChange(version: Short, json: String): EmployeeSalaryChange = version match {
    case 1 => Json.parse(json) match {
      case json"""{"salary":$salary}""" =>
        new EmployeeSalaryChange(newSalary = salary.as[Int])
    }
  }

  def on(evt: EmployeeTitleChange): String = json"""{
  "title": ${evt.newTitle}
}""".toBareString
  def onEmployeeTitleChange(version: Short, json: String): EmployeeTitleChange = version match {
    case 1 => Json.parse(json) match {
      case json"""{"title":$title}""" =>
        new EmployeeTitleChange(newTitle = title.as[String])
    }
  }

}
