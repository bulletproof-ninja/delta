package sampler.aggr.emp

import delta.testing._
import delta.util.ReflectiveDecoder
import sampler._
import scuff.Codec

trait JsonCodec
  extends EmpEventHandler {
  this: ReflectiveDecoder[_, String] =>

  type Return = JSON

  private val IsoDate = """(\d{4})-(\d{2})-(\d{2})""".r

  private val EmployeeRegisteredCodec = new Codec[EmployeeRegistered, JSON] {
    def encode(evt: EmployeeRegistered): String = s"""{
      "name": "${evt.name}",
      "dob": "${evt.dob.toString}",
      "ssn": "${evt.soch}",
      "title": "${evt.title}",
      "salary": ${evt.annualSalary}
    }"""
    def decode(json: String): EmployeeRegistered = {
      val IsoDate(year, month, day) = json.field("dob")
      new EmployeeRegistered(
        name = json.field("name"),
        dob = new MyDate(year.toShort, month.toByte, day.toByte),
        soch = json.field("ssn"),
        title = json.field("title"),
        annualSalary = json.field("salary").toInt)
    }
  }

  def on(evt: EmployeeRegistered): String = EmployeeRegisteredCodec.encode(evt)
  def onEmployeeRegistered(version: Byte, json: String): EmployeeRegistered = version match {
    case 1 => EmployeeRegisteredCodec.decode(json)
  }
  def on(evt: EmployeeSalaryChange): String = s"""{
    "salary": ${evt.newSalary}
  }"""
  def onEmployeeSalaryChange(version: Byte, json: String): EmployeeSalaryChange = version match {
    case 1 => new EmployeeSalaryChange(newSalary = json.field("salary").toInt)
  }

  def on(evt: EmployeeTitleChange): String = s"""{
    "title": "${evt.newTitle}"
  }"""
  def onEmployeeTitleChange(version: Byte, json: String): EmployeeTitleChange = version match {
    case 1 => new EmployeeTitleChange(newTitle = json.field("title"))
  }

}
