package sampler.aggr.emp

import rapture.json._, jsonBackends.jackson._
import delta.util.ReflectiveDecoder
import sampler._
import scuff.Codec

trait JsonCodec
    extends EmpEventHandler {
  this: ReflectiveDecoder[_, String] =>

  type RT = JSON

  private val IsoDate = """(\d{4})-(\d{2})-(\d{2})""".r

  private val EmployeeRegisteredCodec = new Codec[EmployeeRegistered, JSON] {
    def encode(evt: EmployeeRegistered): String = json"""{
  "name": ${evt.name},
  "dob": ${evt.dob.toString},
  "ssn": ${evt.soch},
  "title": ${evt.title},
  "salary": ${evt.annualSalary}
}""".toBareString
    def decode(json: String): EmployeeRegistered = {
      Json.parse(json) match {
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
  }

  def on(evt: EmployeeRegistered): String = EmployeeRegisteredCodec.encode(evt)
  def onEmployeeRegistered(version: Byte, json: String): EmployeeRegistered = version match {
    case 1 => EmployeeRegisteredCodec.decode(json)
  }
  def on(evt: EmployeeSalaryChange): String = json"""{
  "salary": ${evt.newSalary}
}""".toBareString
  def onEmployeeSalaryChange(version: Byte, json: String): EmployeeSalaryChange = version match {
    case 1 => Json.parse(json) match {
      case json"""{"salary":$salary}""" =>
        new EmployeeSalaryChange(newSalary = salary.as[Int])
    }
  }

  def on(evt: EmployeeTitleChange): String = json"""{
  "title": ${evt.newTitle}
}""".toBareString
  def onEmployeeTitleChange(version: Byte, json: String): EmployeeTitleChange = version match {
    case 1 => Json.parse(json) match {
      case json"""{"title":$title}""" =>
        new EmployeeTitleChange(newTitle = title.as[String])
    }
  }

}
