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

  private val EmployeeRegisteredCodecV1 = new Codec[EmployeeRegistered, JSON] {
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

  def on(evt: EmployeeRegistered): String = EmployeeRegisteredCodecV1.encode(evt)
  def onEmployeeRegistered(encoded: Encoded): EmployeeRegistered = encoded.version match {
    case 1 => EmployeeRegisteredCodecV1.decode(encoded.data)
  }
  def on(evt: EmployeeSalaryChange): String = s"""{
    "salary": ${evt.newSalary}
  }"""
  def onEmployeeSalaryChange(encoded: Encoded): EmployeeSalaryChange = encoded.version match {
    case 1 => new EmployeeSalaryChange(newSalary = encoded.data.field("salary").toInt)
  }

  def on(evt: EmployeeTitleChange): String = s"""{
    "title": "${evt.newTitle}"
  }"""
  def onEmployeeTitleChange(encoded: Encoded): EmployeeTitleChange = encoded.version match {
    case 1 => new EmployeeTitleChange(newTitle = encoded.data.field("title"))
  }

}
