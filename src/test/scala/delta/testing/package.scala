package delta

import concurrent._, duration._

package object testing {

  val isDebug = java.lang.management.ManagementFactory
    .getRuntimeMXBean
    .getInputArguments
    .toString.contains("jdwp")

  val AwaitDuration = if (isDebug) 60.hours else 60.seconds

  implicit class F[T](f: Future[T]) {
    def await = Await.result(f, AwaitDuration)
  }

  import language.implicitConversions
  implicit def unit2fut(unit: Unit) = Future successful unit

  implicit class JsonString(private val json: String) extends AnyVal {
    def field(name: String): String = {
      val m = s""""$name"\\s*:\\s*([^,}]+)""".r.findFirstMatchIn(json).get
      val value = m.group(1).trim
      if (value.startsWith("\"") && value.endsWith("\"")) value.substring(1, value.length-1)
      else value
    }
  }
}
