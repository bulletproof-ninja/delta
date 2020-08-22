package delta

import concurrent._, duration._

import scala.util.{ Random => rand }
import scuff._

package object testing {

  val isDebug = java.lang.management.ManagementFactory
    .getRuntimeMXBean
    .getInputArguments
    .toString.contains("jdwp")

  val AwaitDuration = if (isDebug) 16.hours else 360.seconds

  implicit class F[T](private val f: Future[T]) extends AnyVal {
    def await = Await.result(f, AwaitDuration)
  }

  implicit def toFuture[T](t: T) = Future successful t

  def randomName: String = {
    val upper: Char = rand.nextBetween('A', 'Z' + 1)
    val lower = (1 to rand.nextBetween(2, 13)).map(_ => rand.nextBetween('a', 'z' + 1)).map(_.toChar)
    (upper +: lower).mkString
  }

}
