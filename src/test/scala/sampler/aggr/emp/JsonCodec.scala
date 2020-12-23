package sampler.aggr.emp

import delta.util.ReflectiveDecoder
import sampler._
import scuff.Codec
import scuff.json._, JsVal._

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
      val jsObj = JsVal.parse(json).asObj
      val IsoDate(year, month, day) = jsObj.dob.asStr.value
      new EmployeeRegistered(
        name = jsObj.name.asStr,
        dob = new MyDate(year.toShort, month.toByte, day.toByte),
        soch = jsObj.ssn.asStr,
        title = jsObj.title.asStr,
        annualSalary = jsObj.salary.asNum)
    }
  }

  def on(evt: EmployeeRegistered): String = EmployeeRegisteredCodecV1.encode(evt)
  def onEmployeeRegistered(encoded: Encoded): EmployeeRegistered = encoded.version {
    case 1 => EmployeeRegisteredCodecV1.decode(encoded.data)
  }
  def on(evt: EmployeeSalaryChange): String = evt.newSalary.toString
  def onEmployeeSalaryChange(encoded: Encoded): EmployeeSalaryChange = encoded.version {
    case 1 => new EmployeeSalaryChange(newSalary = encoded.data.toInt)
  }

  def on(evt: EmployeeTitleChange): String = JsStr(evt.newTitle).toJson
  def onEmployeeTitleChange(encoded: Encoded): EmployeeTitleChange = encoded.version {
    case 1 => new EmployeeTitleChange(newTitle = JsVal.parse(encoded.data).asStr)
  }

}
