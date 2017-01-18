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

}
