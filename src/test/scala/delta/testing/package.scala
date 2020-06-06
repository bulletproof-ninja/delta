package delta

import concurrent._, duration._

package object testing {

  val isDebug = java.lang.management.ManagementFactory
    .getRuntimeMXBean
    .getInputArguments
    .toString.contains("jdwp")

  val AwaitDuration = if (isDebug) 16.hours else 360.seconds

  implicit class F[T](private val f: Future[T]) extends AnyVal {
    def await() = Await.result(f, AwaitDuration)
  }

  implicit def toFuture[T](t: T) = Future successful t

}
